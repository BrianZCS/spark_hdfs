#! /bin/bash
echo "Running Assignment 1"
echo "You need to configure and start hadoop before running the script"
echo "You need to configure and start spark before running the script"
echo "if not, please quit the run.sh and restart it later"

echo "Running Part 2"
# Part 2
spark-3.3.4-bin-hadoop3/bin/spark-submit part2.py /data/export.csv /data/output.csv