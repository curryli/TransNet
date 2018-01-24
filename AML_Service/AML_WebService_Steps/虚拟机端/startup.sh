#!/bin/bash

nohup java -jar aml-1.0.jar >> ~/nohub.log 2>&1 &

tail -f ~/nohub.log

