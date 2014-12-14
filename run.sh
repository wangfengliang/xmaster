#!/bin/sh

protoc --python_out=. message.proto 
python master.py master.cfg >a.log 2>&1 &

