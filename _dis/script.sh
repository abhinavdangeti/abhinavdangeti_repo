#!/bin/bash
for i in {1..50}:
do
	make RunLoadrunner &
	sleep 600
	killall -9 java
done
