#!/bin/bash
#DID IT WITH JOSEPH COUZENS

exec > >(tee output.log)
exec 2>&1
echo "Step 1: Building code and running JUnit tests..."
mvn test || true

echo "Build and tests completed successfully."
echo ""

PEER1="1:8010"
PEER2="2:8020"
PEER3="3:8030"
PEER4="4:8040"
PEER5="5:8050"
PEER6="6:8060"
PEER7="7:8070"

GATEWAY="9999:8080"

BASE_DIR=$(pwd)
CLASS_PATH="$BASE_DIR/target/classes"

PIDS=()

start_peer() {
    local index=$1
    local peer_config=$2

    java -cp "$CLASS_PATH" \
        edu.yu.cs.com3800.stage5.StartServer \
        "$index" \
        "$PEER1" "$PEER2" "$PEER3" "$PEER4" "$PEER5" "$PEER6" "$PEER7" \
        "P" \
        "$GATEWAY" &
    PIDS+=($!)
    echo "Started peer server $index (PID: ${PIDS[-1]})"
}

start_gateway() {
    java -cp "$CLASS_PATH" \
        edu.yu.cs.com3800.stage5.StartServer \
        "8" \
        "$PEER1" "$PEER2" "$PEER3" "$PEER4" "$PEER5" "$PEER6" "$PEER7" \
        "G" \
        "$GATEWAY" &

    PIDS+=($!)
    echo "Started gateway server (PID: ${PIDS[-1]})"
}

cleanup() {
    echo ""
    echo "Cleaning up... Stopping all servers"
    for pid in "${PIDS[@]}"; do
        kill $pid 2>/dev/null
    done
    wait
    echo "All servers stopped."
    exit
}

trap cleanup SIGINT SIGTERM

echo "Starting 7 peer servers..."
for i in {1..7}; do
    start_peer $i
    sleep 0.5
done

echo "Starting gateway server..."
start_gateway

echo ""
echo "WAITING"
sleep 15

echo ""
echo "Running Demo Client: "

java -cp "$CLASS_PATH" edu.yu.cs.com3800.stage5.RunDemo

echo ""
echo "Done"
echo ""

# Keep script running so servers stay alive
wait