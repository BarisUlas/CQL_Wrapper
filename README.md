# CQL_Wrapper
* wrapper for common CQL operations   

Requires [cassandra docker installation](https://cassandra.apache.org/_/quickstart.html).   
Note for linux: Installation of `Docker engine` may not be sufficient to run the script. Make sure to install the full `Docker Desktop` instead.

### Usage
Run a docker instance with `cassandra:latest` image installed.   
First make it executable: `chmod +x cassandraWrapper.py`   
Then run it: `./cassandraWrapper.py`   
   
Note: The script requires `Docker Desktop` to be running to create cassandra nodes.   

## Basic commands

### Creating a node

To create a new node, simply run the script and it should ask you to create a new node if no existing nodes exists in the Docker network.

After creating a new Cassandra node, the script will wait for the container to be ready and will connect when the initalization completes. The amount of time it takes to run the cluster could depend on your hardware. For an idea, M1 Pro 8C is able to get it running at around 40 seconds.

### Creating a session

Upon creation of the initial node, the script will automatically connect to it by creating a new session. If required, it is possible to create new sessions and/or switch between them.

### Switching between sessions
`session` prefix is used for session-related commands. `session ls` can be used to list existing sessions, and `session switch <session index>` can be used to switch to a different session.

### Creating new sessions
`session new` command can be used to create new cassandra sessions on a single machine. In order to create multiple nodes, make sure that 127.0.0.X route is up where X-1 is the index of the session.    
You can use `sudo ifconfig lo0 alias 127.0.0.x up` command to enable the route for X-1 index node on UNIX-like machines.

### Removing sessions
You can use the command `wipe <node index>/all` to remove a node or all nodes. Alternatively, you can pass `wipe` command at the start of the script to remove all cassandra-related networks from Docker.

### Initializing the keyspace

`ks <replication factor>` can be used to setup a keyspace with the given replication factor. The node will automatically create a keyspace called "DEMO" with the table "demo".

### Inserting elements to the table

`insert (<str1>, <str2>)` command can used to insert elements to the table. 

### Listing elements inside the table

`print` command can be used to print the contents of the table.

### Tracing the commands

`tracing on/off` can be used to enable or disable tracing. 
Example usage:   
```py
> tracing on
Tracing is now enabled

> print
Executing cmd: SELECT * FROM demo.DEMO;
Consistency level: 1

====================[ START TRACING LOG ]====================

Request Type: Execute CQL3 query
Duration: 0:00:00.002068
Coordinator: 172.18.0.2
Started At: 2023-12-27 11:27:46.419000
Parameters: {'consistency_level': 'ONE', 'page_size': '5000', 'query': 'SELECT * FROM demo.DEMO;', 'serial_consistency_level': 'SERIAL'}
Client: 192.168.65.1
Parsing SELECT * FROM demo.DEMO; on 172.18.0.2[Native-Transport-Requests-1] at 2023-12-27 11:27:46.419001
Preparing statement on 172.18.0.2[Native-Transport-Requests-1] at 2023-12-27 11:27:46.419002
Computing ranges to query on 172.18.0.2[Native-Transport-Requests-1] at 2023-12-27 11:27:46.419003
Submitting range requests on 17 ranges with a concurrency of 1 (0.0 rows per range expected) on 172.18.0.2[Native-Transport-Requests-1] at 2023-12-27 11:27:46.420000
Submitted 1 concurrent range requests on 172.18.0.2[Native-Transport-Requests-1] at 2023-12-27 11:27:46.420001
Executing seq scan across 0 sstables for (min(-9223372036854775808), min(-9223372036854775808)] on 172.18.0.2[ReadStage-2] at 2023-12-27 11:27:46.420002
Read 1 live rows and 0 tombstone cells on 172.18.0.2[ReadStage-2] at 2023-12-27 11:27:46.420003

====================[ END TRACING LOG ]====================
Row(userid='foo', meeting_time='bar')

> 
```
