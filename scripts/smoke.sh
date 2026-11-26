#!/usr/bin/env bash
# End-to-end check of a ferrum-db started with `docker compose up -d`, using only
# bash: write data, crash the container hard, restart it, read the data back.
set -euo pipefail

HOST=${FERRUM_HOST:-127.0.0.1}
PORT=${FERRUM_HOST_PORT:-6380}

# resp CMD ARG... : sends one command and prints the first reply line.
resp() {
  local payload="*$#\r\n" arg
  for arg in "$@"; do payload+="\$${#arg}\r\n${arg}\r\n"; done
  exec 3<>"/dev/tcp/${HOST}/${PORT}"
  printf '%b' "$payload" >&3
  local line
  IFS= read -r -t 5 line <&3
  exec 3>&- 3<&-
  printf '%s\n' "${line%$'\r'}"
}

wait_ready() {
  local _
  for _ in $(seq 1 60); do
    if [[ "$(resp PING 2>/dev/null || true)" == "+PONG" ]]; then return 0; fi
    sleep 0.5
  done
  echo "server did not become ready" >&2
  return 1
}

wait_ready
[[ "$(resp SET smoke:key hello)" == "+OK" ]]
before=$(resp INCR smoke:counter)
[[ "${before}" =~ ^:[0-9]+$ ]]
echo "wrote data (counter reply ${before}); crashing the container with SIGKILL"
docker compose kill -s SIGKILL ferrum
docker compose up -d ferrum
wait_ready
after=$(resp INCR smoke:counter)
expected=$((${before#:} + 1))
[[ "${after}" == ":${expected}" ]] || { echo "counter lost: got ${after}, wanted :${expected}" >&2; exit 1; }
# GET answers with a bulk header ($5) followed by the data line.
[[ "$(resp GET smoke:key)" == '$5' ]]
echo "ok: data survived a SIGKILL restart"
