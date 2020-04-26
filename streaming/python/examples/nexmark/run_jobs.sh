#!/bin/bash


# Max throughputs:
# Query 1: 200k
# Query 2: 400k
# Query 3: 100k
# Query 5: 200k
# Query 8: 40k

for QUERY in 1; do
    for THROUGHPUT in 100000 200000; do
        bash run_job.sh $QUERY $THROUGHPUT
    done
done
for QUERY in 2; do
    for THROUGHPUT in 100000 200000 300000 400000 ; do
        bash run_job.sh $QUERY $THROUGHPUT
    done
done

for QUERY in 3 5 8; do
    for THROUGHPUT in 10000 20000 30000 40000 50000; do
        bash run_job.sh $QUERY $THROUGHPUT
    done
done
