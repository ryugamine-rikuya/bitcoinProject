#!/bin/bash
nohup tshark -i en0 -b duration:10 -w bitcoin01 &
