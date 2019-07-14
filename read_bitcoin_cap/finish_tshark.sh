#!/bin/bash

PS=`ps aux | grep tshark | grep -v grep | grep -v finish_tshark.sh` 
IFS='     '
set -- $PS
echo $2
kill -9 $2
