# CS557: Assignment 3 - Replicated Key-Value Store with Configurable Consistency

## Authors: Abha Chaudhary and Kenneth Peter Fernandes

---

## Tools & Programming Language:
- Tools: Thrift
- Programming Language: Java
- Devlopment IDE: Visual Studio Code.

---

## Compilation and Execution process:
- Navigate to the folder "cs457-557-fall2020-pa3-achaud15-kferna11/java" and run the following commands:
```commandline
$> bash
$> export PATH=$PATH:/home/cs557-inst/local/bin
```
- Compile the "keyvalstore.thrift" IDL file to Java by using the following commands:
```commandline
$> thrift -gen java keyvalstore.thrift
```
- The gen-java folder would be generated.
- Compile the programs by executing the following command:
```commandline
$> make
```
- To execute the server, we need to open 4 terminals and goto the same path mentioned above
- Execute the following commands:
```commandline
$> chmod u+x server.sh
$> ./server.sh <PORT_NUMBER>
```
- To execute the client, we need to open terminals and goto the same path mentioned above.
- Provide ip address, port number and consistency level ONE or QUORUM.
- Execute the following commands:
```commandline
$> chmod u+x client.sh
$> ./server.sh <IP_ADDRESS> <PORT_NUMBER> <CONSISTENCY_LEVEL>
```
---

## Assignment completion status:
- Implementation and execution of test cases:
 1. When all replica nodes are active and performing a put request.
 2. When all replica nodes are active and performing a get request.
 3. When one replica node is active and performing a put/get request for consistency level ONE.
 4. When 2 or more replica node is active and performing a put/get request for consistency level QUORUM.
 5. When less than 2 replica nodes are active for put/get request for consistency level QUORUM, exception is thrown.
 6. When none of the replica nodes are active for put/get request for consistency level ONE, exception is thrown.
 7. When any replica node fails for put request, then after recovery latest value has been updated - Hinted Handoff.

- Executed the test cases on remote.cs.binghamton.edu server.
