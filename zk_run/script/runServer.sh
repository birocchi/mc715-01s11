#!/bin/bash

n=$(uname -n)
if [ $n = 'cluster1.lab.ic.unicamp.br' ]
then
  n='node01.cluster'
fi

n=${n%.cluster} 
n=${n#node}

if [ $# -eq 0 ]
then
    echo "usage: $0 [start | stop]"    
    exit
fi

if [ $1 = "start" ]
then
    echo "Starting server for node $n"
elif [ $1 = "stop" ]
then
    echo "Stoping server for node $n"
elif [ $1 = "node" ]
then
    echo "You are at node $n"
    exit
else
    echo "usage: $0 [start | stop]"  
    exit
fi

bin_path=$(dirname $0)/../bin/

$bin_path/zkServer.sh $1 zk$n.cfg
