#! /bin/bash
echo "You need to configure and start hadoop before running the script"
echo "You need to configure and start spark before running the script"
echo "remember to intall spark in /mnt/data"
echo "if not, please quit the run.sh and restart it later"

echo "Running Part 3 (Task 2)"
# Part 3 (Task 2)
cd /mnt/data
spark-3.3.4-bin-hadoop3/bin/spark-submit ./task2/pageRank_wiki_partition.py
