#!/bin/bash +vx
#LIB_PATH=/home/cs557-inst/local/lib/libthrift-0.13.0.jar:/home/cs557-inst/local/lib/slf4j-api-1.7.30.jar:/home/cs557-inst/loca/lib/slf4j-simple-1.7.30.jar:/home/cs557-inst/local/lib/javax.annotation-api-1.3.2.jar
LIB_PATH=lib/libthrift-0.13.0.jar:lib/slf4j-api-1.7.30.jar:lib/slf4j-simple-1.7.30.jar:lib/javax.annotation-api-1.3.2.jar
#port
java -classpath bin/server_classes:$LIB_PATH Server $1