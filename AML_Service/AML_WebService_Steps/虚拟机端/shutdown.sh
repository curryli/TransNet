#!/bin/bash

pid=`lsof -i :8080|grep LISTEN|grep java|awk '{print $2}'`

if [ $pid ] ; then
    kill -9 $pid
    echo $pid" is killed."
else
    echo "no pid on 8080."
fi

