#!/bin/bash

# URL of the Rust server's endpoint
URL="http://127.0.0.1:7878"

# Number of segment summaries to create
NUM_REQUESTS=1000

# Number of concurrent requests per batch
BATCH_SIZE=1

# Calculate the number of batches
NUM_BATCHES=$((NUM_REQUESTS / BATCH_SIZE))
REMAINDER=$((NUM_REQUESTS % BATCH_SIZE))

# Function to send a single request
send_request() {
    curl -s "$URL" > /dev/null
}

export -f send_request

# Loop to send requests in batches
for ((i=1;i<=NUM_BATCHES;i++))
do
    echo "Sending batch $i..."
    for ((j=1;j<=BATCH_SIZE;j++))
    do
        send_request &
    done
    wait  # Wait for all background requests in the batch to complete
    echo "Batch $i completed. Waiting for 2 seconds..."
    sleep 2
done

# Send any remaining requests
if [ $REMAINDER -ne 0 ]; then
    echo "Sending remaining $REMAINDER requests..."
    for ((j=1;j<=REMAINDER;j++))
    do
        send_request &
    done
    wait
    echo "Remaining requests completed."
fi

echo "Completed sending $NUM_REQUESTS requests."