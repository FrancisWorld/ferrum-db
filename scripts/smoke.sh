#!/usr/bin/env bash
# End-to-end check of the compose service using nothing but bash: start it with a freshly
# generated password, write data, crash the container hard, restart it, read the data back.
# The password lives only in this process's environment; it is never written to disk.
#
# SMOKE_COMPOSE_FLAGS overrides the flags of `docker compose up -d` (default: --build).
set -euo pipefail

HOST=${FERRUM_HOST:-127.0.0.1}
PORT=${FERRUM_HOST_PORT:-6380}
FERRUM_REQUIREPASS=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
export FERRUM_REQUIREPASS FERRUM_HOST_PORT=$PORT

# resp CMD ARG... : authenticates, sends one command and prints the first reply line.
resp() {
  local payload="" arg
  payload+="*2\r\n\$4\r\nAUTH\r\n\$${#FERRUM_REQUIREPASS}\r\n${FERRUM_REQUIREPASS}\r\n"
  payload+="*$#\r\n"
  for arg in "$@"; do payload+="\$${#arg}\r\n${arg}\r\n"; done
  exec 3<>"/dev/tcp/${HOST}/${PORT}"
  printf '%b' "$payload" >&3
  local auth line
  IFS= read -r -t 5 auth <&3
  [[ "${auth%$'\r'}" == "+OK" ]] || { exec 3>&- 3<&-; echo "AUTH failed: ${auth}" >&2; return 1; }
  IFS= read -r -t 5 line <&3
  exec 3>&- 3<&-
  printf '%s\n' "${line%$'\r'}"
}

# An unauthenticated client must be refused.
noauth() {
  exec 3<>"/dev/tcp/${HOST}/${PORT}"
  printf '*1\r\n$4\r\nPING\r\n' >&3
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

# shellcheck disable=SC2086
docker compose up -d ${SMOKE_COMPOSE_FLAGS:---build}
wait_ready
[[ "$(noauth)" == -NOAUTH* ]] || { echo "server answered without authentication" >&2; exit 1; }
[[ "$(resp SET smoke:key hello)" == "+OK" ]]
before=$(resp INCR smoke:counter)
[[ "${before}" =~ ^:[0-9]+$ ]]
echo "wrote data (counter reply ${before}); crashing the container with SIGKILL"
docker compose kill -s SIGKILL ferrum
# shellcheck disable=SC2086
docker compose up -d ${SMOKE_COMPOSE_FLAGS:---build}
wait_ready
after=$(resp INCR smoke:counter)
expected=$((${before#:} + 1))
[[ "${after}" == ":${expected}" ]] || { echo "counter lost: got ${after}, wanted :${expected}" >&2; exit 1; }
# GET answers with a bulk header ($5) followed by the data line.
[[ "$(resp GET smoke:key)" == '$5' ]]
echo "ok: data survived a SIGKILL restart"
