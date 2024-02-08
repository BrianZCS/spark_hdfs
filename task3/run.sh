#! /bin/bash
echo "You need to configure and start hadoop before running the script"
echo "You need to configure and start spark before running the script"
echo "if not, please quit the run.sh and restart it later"

echo "Running Part 3 (Task 3)"
# Part 3 (Task 3)
spark-3.3.4-bin-hadoop3/bin/spark-submit pageRank_wiki_memory.py