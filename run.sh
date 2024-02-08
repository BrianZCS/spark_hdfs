#! /bin/bash
echo "Running Assignment 1"
echo "You need to configure and start hadoop before running the script"
echo "You need to configure and start spark before running the script"
echo "if not, please quit the run.sh and restart it later"

echo "Running Part 2"
# Part 2
spark-3.3.4-bin-hadoop3/bin/spark-submit part2.py /data/export.csv /data/output.csv

echo "Running Part 3 (Task 1)"
# Part 3 (Task 1)
spark-3.3.4-bin-hadoop3/bin/spark-submit pageRank_wiki.py

echo "Running Part 3 (Task 2)"
# Part 3 (Task 2)
spark-3.3.4-bin-hadoop3/bin/spark-submit pageRank_wiki_partition.py

echo "Running Part 3 (Task 3)"
# Part 3 (Task 3)
spark-3.3.4-bin-hadoop3/bin/spark-submit pageRank_wiki_memory.py

echo "Running Part 3 (Task 4)"
# Part 4 (Task 4)
spark-3.3.4-bin-hadoop3/bin/spark-submit pageRank_wiki_worker_kill.py
echo "Killing Worker"
for pid in $(jps | grep Worker | awk '{print $1}'); do kill -9 $pid; done