#! /bin/bash
echo "You need to configure and start hadoop before running the script"
echo "You need to configure and start spark before running the script"
echo "remember to intall spark in /mnt/data"
echo "if not, please quit the run.sh and restart it later"
echo "remember to copy the killing workers code"
echo "for pid in $(jps | grep Worker | awk '{print $1}'); do kill -9 $pid; done"

echo "Running Part 3 (Task 4)"
# Part 4 (Task 4)
cd /mnt/data
spark-3.3.4-bin-hadoop3/bin/spark-submit ./task4/pageRank_wiki_worker_kill.py
echo "Killing Worker"
for pid in $(jps | grep Worker | awk '{print $1}'); do kill -9 $pid; done