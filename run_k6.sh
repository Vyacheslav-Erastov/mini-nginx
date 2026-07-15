docker run --rm \
  --network host \
  -v "$PWD/tests:/scripts" \
  grafana/k6 \
  run \
  -e "TEST_TYPE=${TEST_TYPE:-load}" \
  -e "BASE_URL=${BASE_URL:-http://127.0.0.1:8090}" \
  -e "DELAY_SECONDS=${DELAY_SECONDS:-20}" \
  /scripts/load_test.js
