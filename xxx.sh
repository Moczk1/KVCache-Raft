#!/bin/bash

./bin/server \
  -n 3 \
  -f ./test.conf \
  -r ./bin/raftfile.txt \
  -s ./bin/snapshot.txt \
  >server.log 2>&1 &

SERVER_PID=$!

sleep 15

RAFT_PIDS=$(pgrep -P "$SERVER_PID" | paste -sd, -)
echo "server parent: $SERVER_PID"
echo "raft nodes: $RAFT_PIDS"



perf record \
  -F 99 \
  -g \
  --call-graph fp \
  -p "$RAFT_PIDS" \
  -o perf-server.data \
  -- sleep 30 &

PERF_PID=$!
sleep 1

for i in $(seq 1 8); do
  ./bin/clerk >"/tmp/clerk-$i.log" 2>&1 &
done

wait "$PERF_PID"



perf stat -p "$RAFT_PIDS" \
  -e task-clock,cycles,instructions,context-switches,cpu-migrations,page-faults \
  -- sleep 30