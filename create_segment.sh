#!/bin/bash

# URLs of the Rust server's endpoints
CREATE_URL="http://127.0.0.1:7878/create_segment"
QUERY_URL="http://127.0.0.1:7878/query_segment"

# Number of segment summaries to create
NUM_CREATE_REQUESTS=1000

# Number of create requests per batch
CREATE_BATCH_SIZE=1

# Array of terms to query
TERMS=("10.6089/jscm.9.49,http://joi.jlc.jst.go.jp/JST.Journalarchive/jscm1975/9.49?from=CrossRef" "banana" "10.6089/jscm.9.8,http://joi.jlc.jst.go.jp/JST.Journalarchive/jscm1975/9.8?from=CrossRef" "date" "elderberry" "fig" "grape" "10.1001/archderm.109.2.212,http://archderm.ama-assn.org/cgi/doi/10.1001/archderm.109.2.212" "kiwi" "10.6089/jscm.9.92,http://joi.jlc.jst.go.jp/JST.Journalarchive/jscm1975/9.92?from=CrossRef")

# Function to send a single create_segment request
send_create_request() {
    curl -s -X POST "$CREATE_URL" > /dev/null
}

# Function to send a single query_segment request with a specific id
send_query_request() {
    local id=$1
    # Select a random term from TERMS array
    local RANDOM_INDEX=$(( RANDOM % ${#TERMS[@]} ))
    local RANDOM_TERM=${TERMS[$RANDOM_INDEX]}
    # Send GET request
    curl -s "$QUERY_URL?id=$id&term=$RANDOM_TERM" > /dev/null
    echo "Queried segment $id with term '$RANDOM_TERM'."
}

export -f send_create_request
export -f send_query_request
export CREATE_URL
export QUERY_URL
export NUM_CREATE_REQUESTS
export TERMS

echo "Starting to create and query $NUM_CREATE_REQUESTS segments..."

# Loop to send create_segment and query_segment requests in batches
for ((i=1;i<=NUM_CREATE_REQUESTS;i++))
do
    send_create_request &
    send_query_request "$i" &
    
    # Control concurrency
    if (( i % CREATE_BATCH_SIZE == 0 )); then
        wait
        echo "Processed $i segments."
        echo "Waiting for 2 seconds..."
        sleep 2
    fi
done

# Handle any remaining requests
if (( NUM_CREATE_REQUESTS % CREATE_BATCH_SIZE != 0 )); then
    wait
    echo "Processed all $NUM_CREATE_REQUESTS segments."
fi

echo "Completed creating and querying $NUM_CREATE_REQUESTS segments."
echo "Bash script execution finished."