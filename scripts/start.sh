#!/bin/sh

trap "exit" INT TERM ERR
trap "kill 0" EXIT


node lib/producer.js --name 1 &
node lib/worker.js --name 1 --group 1 &
node lib/worker.js --name 2 --group 1 &
node lib/worker.js --name 3 --group 2 &

wait
