import os
import sys
import subprocess

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
    query_output = runCQLQuery("DESCRIBE KEYSPACES;")
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
        
def setTracing(toggle: bool):
    cmd = "tracing on" if toggle else "tracing off"
    runCQLQuery(cmd)

def printUsage():
    print("\nUsage: python3 cassandraWrapper.py [OPTIONS]")
    print("OPTIONS:\n\
        --start: Start Cassandra\n\
        --shell: Start interactive shell\n\
        --query: Run a CQL query\n\
        --tracing=true/false: Enable/disable tracing\n\
        --init-keyspace: Initalize keyspace\n\
        --init-table: Initalize table\n\
        --insert-data: Insert hardcoded data\n\
        --insert-null: Insert null data\n\
        --print-table: Print table contents\n\
        --set-null: Delete null data\n\
        --close: Kill cassandra and remove container from network\n\
    ")

def main():
    argv_array = sys.argv
    keyspace = ""

    if len(argv_array) == 1:
        printUsage()
        return

    for argv in sys.argv:
        if "--shell" in argv:
            interactiveShell()
            return

        elif "--query" in argv:
            cql_query = input("Enter CQL query: ")
            runCQLQuery(cql_query)
            return

        elif "--start" in argv:
            startCassandra()
            return

        elif "--init-keyspace" in argv:
            keyspace_str = argv[16:]
            # try with 2 or 3 replicas
            # consistency level = 2 or 3
            cql_query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace_str + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
            runCQLQuery(cql_query)
            keyspace = keyspace_str
            return
        
        elif "--tracing" in argv:
            toggle = bool(argv[9:])
            output = setTracing(toggle)
            print(output)
            return

        elif "--insert-random" in argv:
            range_str = argv[16:]
            for i in range(int(range_str)):
                cql_query = f"INSERT INTO {getKeyspace()}.DEMO (userid, meeting_time) VALUES ('{i}', '{i}');"
                runCQLQuery(cql_query)
            return
        
        elif "--init-table" in argv:
            cql_query = "CREATE TABLE IF NOT EXISTS " + getKeyspace() + ".DEMO (\
                        userid text PRIMARY KEY,\
                        meeting_time text\
                        );"
            runCQLQuery(cql_query)
            return
        
        elif "--insert-data" in argv:
            data_str = argv[14:]
            data_arr = data_str.split(',')
            cql_query = f"INSERT INTO {getKeyspace()}.DEMO (userid, meeting_time) VALUES ('{data_arr[0]}', '{data_arr[1]}');"
            runCQLQuery(cql_query)
            return

        elif "--print-table" in argv:
            cql_query = "SELECT * FROM " + getKeyspace() + ".DEMO;"
            runCQLQuery(cql_query)
            return

        elif "--insert-null" in argv:
            cql_query = "INSERT INTO " + getKeyspace() + ".DEMO (userid, meeting_time) VALUES ('null', 'null');"
            runCQLQuery(cql_query)
            return
        
        elif "--delete-null" in argv:
            cql_query = "DELETE FROM " + getKeyspace() + ".DEMO WHERE userid = 'null';"
            runCQLQuery(cql_query)
            return

        elif "--close" in argv:
            runOScmd("docker kill cassandra && docker network rm cassandra", stdout=True)
            return

    print("Invalid argument: " + argv_array[1])
    printUsage()    


if __name__ == "__main__":
    main()