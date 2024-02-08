#! /bin/bash
echo "You need to configure and start hadoop before running the script"
echo "You need to configure and start spark before running the script"
echo "if not, please quit the run.sh and restart it later"

echo "Running Part 3 (Task 4)"
# Part 4 (Task 4)
spark-3.3.4-bin-hadoop3/bin/spark-submit pageRank_wiki_worker_kill.py
echo "Killing Worker"
for pid in $(jps | grep Worker | awk '{print $1}'); do kill -9 $pid; done