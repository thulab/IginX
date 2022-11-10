#!/bin/bash

# below JavaApp is the name of running Java process
jps

pids=( $(jps | grep Iginx | awk '{print $1}') )

for pid in "${pids[@]}"; do
     echo "killing $pid"
     kill -9 $pid
done