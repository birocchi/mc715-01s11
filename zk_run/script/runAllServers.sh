#!/bin/bash

dir=$(dirname $0)
runServer=$dir/runServer.sh

nodes=( node01 node03 node04 )

if [[ $1 = "start" || $1 = "stop" ]]
then

    for node in ${nodes[@]}
    do
      ssh $node "cd $PWD;$dir/runServer.sh $1;"
    done

else
    echo "usage: $0 [start | stop]"  
    exit
fi