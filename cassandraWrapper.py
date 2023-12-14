#!/usr/bin/env python3
import subprocess
import sys
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import cassandra.cluster
from threading import Thread, Lock
import random
import string
import docker

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

cassandra_session = None
lookup_consistency_level = ConsistencyLevel.ONE
mutex = Lock()
session_list = []
docker_client = None

def startSession(ip_address, port, cold=False):

    global cassandra_session
    # Connect to the Cassandra cluster
    print("\nAttempting to connect to Cassandra cluster at " + ip_address + " on port " + str(port))
    if cold:
        while True:
            try:
                cluster = Cluster([ip_address], port=port)
                cassandra_session = cluster.connect()
                break
            except Exception as e:
                if "ConnectionShutdown" in e.__str__():
                    print(".", end='', flush=True)
                    time.sleep(1)
                else:
                    print(bcolors.BOLD + bcolors.FAIL + "Unexpected exception occured: " + e.__str__() + bcolors.ENDC)
                    print(bcolors.OKBLUE + bcolors.BOLD + "hint: check if 127.0.0.x is up: " + bcolors.ENDC\
                          + "sudo ifconfig lo0 alias 127.0.0.x up")
                    exit(-1)
    try:
        cluster = Cluster([ip_address], port=port)
        cassandra_session = cluster.connect()
        session_list.append(cassandra_session)
        print(bcolors.OKGREEN + bcolors.BOLD + "\nConnected to Cassandra cluster at " + ip_address + "\nSession ID: " + str(cassandra_session.session_id) + bcolors.ENDC)
    
    except cassandra.cluster.NoHostAvailable:
        print("Node still initializing, please wait")
        startSession(ip_address, port, cold=True)
        return
    except Exception as e:
        print(bcolors.FAIL + bcolors.BOLD + "Failed to connect to Cassandra cluster, check the IP Address and port" + bcolors.ENDC)
        exit(-1)
    
def getCassandraInstanceTuple(current=False):
    # returns (next_cassandra_instance_name, next_cassandra_instance_port, next_cassandra_instance_ip)

    global docker_client    

    last_cassandra_instance = 0
    networks = docker_client.networks.list()
    for network in networks:
        if "cassandra_node_" in network.name:
            last_cassandra_instance = int(network.name.split('_')[-1]) if current else int(network.name.split('_')[-1]) + 1

    return (f"cassandra_node_{str(last_cassandra_instance)}", 9042 + last_cassandra_instance, f"127.0.0.{str(last_cassandra_instance + 1)}")


def createNode(initial=False):

    global docker_client   

    if initial:
        node_name = "cassandra_node_0"
        port = "9042"
        ip = "127.0.0.1"
    else:
        next_tuple = getCassandraInstanceTuple()
        node_name = next_tuple[0]
        port = next_tuple[1]
        ip = next_tuple[2]

    try:
        docker_client.networks.create(node_name, driver="bridge")
        docker_client.containers.run("cassandra", name=node_name, hostname=node_name, detach=True, network=node_name, ports={f'{port}/tcp': port})
    except Exception as e:
        print(bcolors.FAIL + f"Failed to create {node_name} at {ip} on port {port}" + bcolors.ENDC)
        print(e.__str__())
        exit(-1)


def randomString(stringLength=10):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(stringLength))


def insertMultiThreaded():

    global mutex
    #mutex.acquire()
    query = f"INSERT INTO demo.DEMO (userid, meeting_time) VALUES ('{randomString()}', '{randomString()}');"
    session_cmd(query)
    #mutex.release()
    

def close_session():
    global cassandra_session
    cassandra_session.shutdown()
    print("Closed Cassandra session")

def session_cmd(cmd):
    global cassandra_session
    global lookup_consistency_level

    print("Executing cmd: " + bcolors.BOLD + bcolors.OKBLUE + cmd + bcolors.ENDC)
    print("Consistency level: " + bcolors.BOLD + bcolors.OKGREEN + str(lookup_consistency_level) + bcolors.ENDC)
    
    SimpleStatement(cmd, consistency_level=lookup_consistency_level)
    # get output from cmd
    output = ""
    try:
        output = cassandra_session.execute(cmd)
    except Exception as e:
        print(bcolors.FAIL + "Failed to execute cmd: " + e.__str__() + bcolors.ENDC)
        return
    return output


def initKeyspace(replication_factor: int):
    cql_query = "CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : " + str(replication_factor) + " };"
    session_cmd(cql_query)


def runOScmd(cmd, stdout: bool = False):
    print(f'Running cmd: {cmd}') if stdout else None
    subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

def runCQLQuery(query):
    output = ""
    with open('query.cql', 'w') as f:
        f.write(query)

    cmd = f'docker run --rm --network cassandra -v "$(pwd)/query.cql:/scripts/query.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 -e CQLVERSION=3.4.6 nuvo/docker-cqlsh'
    # run cmd using subprocess
    try:
        output = subprocess.check_output(cmd, shell=True, text=True)
    except:
        pass
    return output
    

def getKeyspace():
    query_output = session_cmd("DESCRIBE KEYSPACES;")
    query_output = query_output.split('\n')
    # remove first 4 elements and last 2 elements from query_output list
    query_output = query_output[4:-3]

    complete_output = []
    for output in query_output:
        output = output.split(" ")
        while '' in output:
            output.remove('')
        complete_output += output

    for key in complete_output:
        if "system" not in key:
            return key
        
def switchSession(idx=None):
    global cassandra_session
    global session_list

    if len(session_list) == 0:
        print("No existing sessions")
        return
    
    if idx != None:
        cassandra_session = session_list[idx]
        print("Successfully switched session to: " + bcolors.OKGREEN + bcolors.BOLD + str(session.session_id) + bcolors.ENDC)
        return

    print("Existing sessions:")
    for i in range(len(session_list)):
        print(f"[{i}] {session_list[i].session_id}")

    try:
        session_index = int(input("Enter session index: "))
    except KeyboardInterrupt:
        return
    except:
        print("Invalid input")
        return

    if session_index >= len(session_list):
        print("Invalid session index")
        return

    sescassandra_sessionsion = session_list[session_index]
    print("Successfully switched session to: " + bcolors.OKGREEN + bcolors.BOLD + str(session.session_id) + bcolors.ENDC)

def printUsage():
    print("\nCassandra interactive shell wrapper\n")
    print(bcolors.BOLD + bcolors.UNDERLINE + "COMMANDS:" + bcolors.ENDC + "\n\n\
help                           - print this message\n\
start                          - start a new session\n\
switch [session index]         - switch to another session\n\
ks [replication_factor]        - initialize keyspace with replication factor\n\
initTable                      - initialize table\n\
insert [userid] [meeting_time] - insert data into table\n\
delete [userid]                - delete data from table\n\
print                          - print all data from table\n\
clevel [consistency_level]     - set consistency level\n\
mt                             - multi-threaded insert\n\
stress [num_threads]           - multi-session insertion stress test\n\
    ")

def removeNetwork():
    # use docker library to remove docker containers from network
        global docker_client
        docker_client = docker.from_env()
        docker_network = docker_client.networks.list()
        for network in docker_network:
            if "cassandra_node_" in network.name:
                print("Removing container: " + network.name)
                try:
                    docker_client.networks.get(network.id).remove()
                except Exception as e:
                    print(bcolors.FAIL + "Failed to remove network: " + network.name + bcolors.ENDC)
                    print(e.__str__())
                    continue

def searchExistingCassandraSession():

    # use docker library to check if docker is running
    global docker_client
    global cassandra_session
    
    try:
        docker_client = docker.from_env()
    except:
        print(bcolors.FAIL + "Docker is not running, terminating script" + bcolors.ENDC)
        exit()
    
    # search for existing cassandra container using docker library
    try:
        # list all containers
        containers = docker_client.containers.list()
        cassandra_containers = []

        for idx, container in enumerate(containers):
            if "cassandra" in container.name:
                cassandra_containers.append(container)
                print(f"[{idx}] {container.name}")
        
        print("Found " + str(len(cassandra_containers)) + " Cassandra container(s)")
        if len(cassandra_containers) == 0:
            raise Exception("No Cassandra containers found")
        
        elif True:

            next_tuple = getCassandraInstanceTuple(current=True)
            node_name = next_tuple[0]
            port = next_tuple[1]
            ip = next_tuple[2]

            #startSession(ip, port, node_name)
            startSession("127.0.0.1", "9042")
            return

        idx = input("\nChoose a container to connect to: ")
        docker_client.containers.get(cassandra_containers[int(idx)].id).start()
        return
    
    except:
        try:
            _input = input("No existing Cassandra container found. Would you like to create a new one? (Y/n): ")
        except KeyboardInterrupt:
            exit()
        if _input == "n":
            exit()
        elif _input == "Y" or _input == "y" or _input == "":
            createNode(initial=True)
            # Connect to the Cassandra cluster
            print(bcolors.OKGREEN + "Successfully created a Cassandra container" + bcolors.ENDC)
            print("Waiting for Cassandra cluster to start", end='', flush=True)
            while True:
                try:
                    # get last cassandra instance tuple
                    next_tuple = getCassandraInstanceTuple(current=True)
                    node_name = next_tuple[0]
                    port = next_tuple[1]
                    ip = next_tuple[2]
                    startSession(ip, port, cold=True)
                    
                    global session_list
                    session_list.append(cassandra_session)
                    break
                except Exception as e:
                    print(e.__str__(), flush=True)  
                    print(".", end='', flush=True)
                    time.sleep(1)
                    continue
                    
        return

def main():

    searchExistingCassandraSession()

    keyspace = ""
    _input = ""

    global lookup_consistency_level
    global cassandra_session

    while True:
        try:
            try:
                _input = input("\n> ")
            except KeyboardInterrupt:
                break

            if _input == "help":
                printUsage()
                continue

            elif _input == "exit":
                break

            elif "start" == _input:
                nextNode = getCassandraInstanceTuple()
                node_name = nextNode[0]
                port = nextNode[1]
                ip = nextNode[2]
                createNode()
                startSession(ip, port, cold=True)
                continue

            elif "switch" in _input:
                if len(_input.split(' ')) == 1:
                    switchSession()
                else:
                    switchSession(int(_input.split(' ')[1]))
                continue
        
            elif "stress" in _input:
                thread_count = int(_input.split(' ')[1])
                threads = []
                for i in range(thread_count):
                    threads.append(Thread(target=insertMultiThreaded))
                    threads[i].start()
                for i in range(thread_count):
                    threads[i].join()
                continue

            elif "mt" in _input:
                thread_count = int(_input.split(' ')[1])
                threads = []
                for i in range(thread_count):
                    threads.append(Thread(target=insertMultiThreaded))
                    threads[i].start()
                for i in range(thread_count):
                    threads[i].join()
                continue

            elif "clevel" in _input:
                temp_consistency_level = _input.split(' ')[1]
                temp_consistency_level = temp_consistency_level.upper()
                lookup_consistency_level = ConsistencyLevel.__dict__[temp_consistency_level]
                print("Consistency level set to: " + bcolors.OKBLUE + bcolors.BOLD + str(lookup_consistency_level) + bcolors.ENDC)
                continue

            elif "ks" in _input:
                keyspace_str = _input.split(' ')[1]
                print(keyspace_str)
                initKeyspace(int(keyspace_str))
                continue

            elif "grace" in _input:
                grace = _input.split(' ')[1]
                cql_query = f"ALTER TABLE demo.DEMO WITH gc_grace_seconds = '{grace}';"
                session_cmd(cql_query)
                continue
            
            elif "tracing" in _input:
                toggle = _input.split(' ')[1] == "on"
                _input = _input.upper()
                session_cmd(_input + ";")
                continue
            
            elif "initTable" == _input:
                cql_query = "CREATE TABLE IF NOT EXISTS demo.DEMO (\
                            userid text PRIMARY KEY,\
                            meeting_time text\
                            );"
                session_cmd(cql_query)
                continue
            
            elif "insert" in _input:
                data_arr = _input.split(' ')[1:]
                query = f"INSERT INTO demo.DEMO (userid, meeting_time) VALUES ('{data_arr[0]}', '{data_arr[1]}');"
                print(query)
                session_cmd(query)
                continue

            elif "delete" in _input:
                data_arr = _input.split(' ')
                query = f"DELETE FROM demo.DEMO WHERE userid = '{data_arr[1]}';"
                session_cmd(query)
                continue

            elif "print" in _input:
                cql_query = "SELECT * FROM demo.DEMO;"
                output = session_cmd(cql_query)
                for row in output:
                    print(row)
                continue
            else:
                print("Invalid command, type help for usage")
                continue
        except Exception as e:
            print(bcolors.WARNING + bcolors.UNDERLINE + "An expection occured\n\n" + e.__str__() + bcolors.ENDC)
            continue
    
    print("\nClosing Cassandra session")
    close_session()

    try:
        opt = input("Would you like to remove Cassandra node(s) from Docker network? (y/N): ")
    except KeyboardInterrupt:
        opt = ""

    if opt == "y" or opt == "Y":

        removeNetwork()

        exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "clear":
        removeNetwork()
        exit(0)
    main()