#!/usr/bin/env python3
import os
import sys
import subprocess
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

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

session = None
lookup_consistency_level = ConsistencyLevel.ONE


def start_session():

    global session
    # Connect to the Cassandra cluster
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        print("Connected to Cassandra cluster\nSession: " + str(session))
    except:
        print("Failed to connect to Cassandra cluster, is it exposed and running?")
        return
    
def properlyStartCassandra():
    cmd = "docker network create cassandra_exposed"
    subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    cmd = "docker run --rm -d -p 9042:9042 --name cassandra_exposed --hostname cassandra_exposed --network cassandra_exposed cassandra"
    subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def close_session():
    global session
    session.shutdown()
    print("Closed Cassandra session")

def session_cmd(cmd):
    global session
    global lookup_consistency_level

    print("Executing cmd: " + bcolors.BOLD + bcolors.OKBLUE + cmd + bcolors.ENDC)
    print("Consistency level: " + bcolors.BOLD + bcolors.OKGREEN + str(lookup_consistency_level) + bcolors.ENDC)
    
    SimpleStatement(cmd, consistency_level=lookup_consistency_level)
    # get output from cmd
    output = ""
    try:
        output = session.execute(cmd)
    except Exception as e:
        print(bcolors.FAIL + "Failed to execute cmd: " + e.__str__() + bcolors.ENDC)
        return
    return output


def initKeyspace(replication_factor: int):
    cql_query = "CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : " + str(replication_factor) + " };"
    session_cmd(cql_query)

def runOScmd(cmd, stdout: bool = False):
    print(f'Running cmd: {cmd}') if stdout else None
    os.system(cmd)

def startCassandra():
    # check if cassandra network exists
    cmd = "docker network ls | grep cassandra"
    try:
        subprocess.check_output(cmd, shell=True, text=True)
    except:
        cmd = "docker network create cassandra"
        os.system(cmd)
    os.system("docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra")
    

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
    

def interactiveShell():
    cmd = "docker run --rm -it --network cassandra nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.6'"
    os.system(cmd)

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

def printUsage():
    print("\nCassandra interactive shell wrapper\n")
    print("COMMANDS:\n\
        ks [replication_factor] - initialize keyspace with replication factor\n\
        initTable - initialize table\n\
        insert [userid] [meeting_time] - insert data into table\n\
        delete [userid] - delete data from table\n\
        print - print all data from table\n\
        clevel [consistency_level] - set consistency level\n\
        help - print this message\n\
    ")

def searchExistingCassandraSession():
    # search for existing cassandra container
    cmd = "docker ps | grep cassandra"
    err = ""
    output = ""
    try:
        result = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err = result.stderr
        output = result.stdout

    except:
        print("an error occured while checking for Docker, terminating script")
        exit()

    if err != "":
        print(bcolors.FAIL + "Docker is not running, terminating script" + bcolors.ENDC)
        exit()

    if output != "":
        print("Found existing Cassandra container, attempting to connect...")        
        start_session()
        return
    
    else:
        _input = input("No existing Cassandra container found. Would you like to create a new one? (Y/n): ")
        if _input == "n":
            exit()
        elif _input == "Y" or _input == "y" or _input == "":
            properlyStartCassandra()
            global session
            # Connect to the Cassandra cluster
            print(bcolors.OKGREEN + "Successfully created a Cassandra container" + bcolors.ENDC)
            print("Waiting for Cassandra cluster to start", end='', flush=True)
            while True:
                try:
                    # specify port 9043 to connect to exposed cassandra
                    cluster = Cluster(['127.0.0.1'], port=9042)
                    session = cluster.connect()
                    print("\n" + bcolors.OKGREEN + "Connected to Cassandra cluster!\nSession: " + str(session) + bcolors.ENDC)
                    break
                except Exception as e:
                    print(".", end='', flush=True)
                    time.sleep(1)
                    continue
                    
        return

def main():
    searchExistingCassandraSession()

    keyspace = ""
    _input = ""

    global lookup_consistency_level
    global session

    while _input != "exit":
        try:
            try:
                _input = input("\n> ")
            except KeyboardInterrupt:
                break

            if _input == "help":
                printUsage()
                continue

            elif "clevel" in _input:
                lookup_consistency_level = _input.split(' ')[1]
                lookup_consistency_level = lookup_consistency_level.upper()
                lookup_consistency_level = ConsistencyLevel.__dict__[lookup_consistency_level]
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

    opt = input("Would you like to remove Cassandra from Docker network? (y/N): ")
    if opt == "y" or opt == "Y":
        runOScmd("docker kill cassandra_exposed && docker network rm cassandra_exposed", stdout=True)
        exit()


if __name__ == "__main__":
    main()