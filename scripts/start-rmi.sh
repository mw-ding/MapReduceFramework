#/bin/sh

echo "start java-rmi at port $1" 
rmiregistry $1 &
