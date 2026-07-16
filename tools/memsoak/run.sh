#!/usr/bin/env bash
# memsoak — memory soak harness for outpace (and any /proc-visible target).
#
# Launches `outpace serve` with a fresh isolated data-dir, drives real players
# that actually decode bytes, and samples process memory + per-stream status
# over time into CSVs. See tools/memsoak/README.md.
set -euo pipefail

# ---- defaults ---------------------------------------------------------------
MODE="play"                 # idle | play | churn
IDS=""                      # comma-separated content ids
DURATION=300                # seconds
INTERVAL=5                  # sample period seconds
TRANSPORT="ts"              # ts | m3u8
PLAYERS=1                   # concurrent decoders per id (play mode)
CHURN_HOLD=30               # churn: seconds a stream stays up before cycling
HOST="127.0.0.1"
PORT=6878
RTMP_PORT=0                 # 0 => derive from PORT (isolated from default 1935)
PEER_PORT=0                 # 0 => derive from PORT (isolated from default 8621)
BINARY=""                   # default resolved below
OUTDIR=""
ENVOVERRIDES=""             # e.g. "OUTPACE_ENABLE_INBOUND=0,OUTPACE_ENABLE_SEEDING=0"
ATTACH_PID=""               # sample this pid instead of launching
LABEL="run"

usage() { grep '^# ' "$0" | sed 's/^# //'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --ids) IDS="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --interval) INTERVAL="$2"; shift 2;;
    --transport) TRANSPORT="$2"; shift 2;;
    --players) PLAYERS="$2"; shift 2;;
    --churn-hold) CHURN_HOLD="$2"; shift 2;;
    --host) HOST="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --rtmp-port) RTMP_PORT="$2"; shift 2;;
    --peer-port) PEER_PORT="$2"; shift 2;;
    --binary) BINARY="$2"; shift 2;;
    --out) OUTDIR="$2"; shift 2;;
    --env) ENVOVERRIDES="$2"; shift 2;;
    --attach-pid) ATTACH_PID="$2"; shift 2;;
    --label) LABEL="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "unknown arg: $1" >&2; usage;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[[ -z "$BINARY" ]] && BINARY="$REPO_ROOT/target/release/outpace"
BASE_URL="http://$HOST:$PORT"
# Isolate ancillary listeners from engine defaults (1935 rtmp, 8621 peer) so this
# harness never collides with another daemon on the machine.
[[ "$RTMP_PORT" == "0" ]] && RTMP_PORT=$((PORT + 5000))
[[ "$PEER_PORT" == "0" ]] && PEER_PORT=$((PORT + 6000))

if [[ -z "$OUTDIR" ]]; then
  OUTDIR="$REPO_ROOT/tools/memsoak/results/$(date +%Y%m%d-%H%M%S)-$LABEL-$MODE"
fi
mkdir -p "$OUTDIR"

PROC_CSV="$OUTDIR/process.csv"
STREAM_CSV="$OUTDIR/streams.csv"
META="$OUTDIR/meta.txt"
DAEMON_LOG="$OUTDIR/daemon.log"
DATA_DIR="$OUTDIR/data"

echo "process,rss_kb,hwm_kb,pss_kb,threads,fds,mem_allocated,mem_resident,mem_retained" >/dev/null
echo "ts_unix,elapsed_s,rss_kb,hwm_kb,pss_kb,threads,fds,mem_allocated,mem_resident,mem_retained" >"$PROC_CSV"
echo "ts_unix,elapsed_s,id,http_code,clients,peers,bitrate,buffer_ms,uploaded,frames" >"$STREAM_CSV"

PIDS_TO_KILL=()
DAEMON_PID=""

cleanup() {
  set +e
  for p in "${PIDS_TO_KILL[@]:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null; done
  if [[ -n "$DAEMON_PID" ]]; then
    kill "$DAEMON_PID" 2>/dev/null
    for _ in $(seq 1 20); do kill -0 "$DAEMON_PID" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$DAEMON_PID" 2>/dev/null
  fi
}
trap cleanup EXIT INT TERM

# ---- launch daemon (unless attaching) --------------------------------------
if [[ -n "$ATTACH_PID" ]]; then
  TARGET_PID="$ATTACH_PID"
  echo "attaching to pid $TARGET_PID" | tee "$META"
else
  [[ -x "$BINARY" ]] || { echo "binary not found/executable: $BINARY" >&2; exit 1; }
  mkdir -p "$DATA_DIR"
  ENVARGS=()
  if [[ -n "$ENVOVERRIDES" ]]; then
    IFS=',' read -ra KVS <<<"$ENVOVERRIDES"
    for kv in "${KVS[@]}"; do ENVARGS+=("$kv"); done
  fi
  echo "launching: OUTPACE_BIND=$HOST:$PORT OUTPACE_RTMP_BIND=127.0.0.1:$RTMP_PORT OUTPACE_PEER_LISTEN=0.0.0.0:$PEER_PORT OUTPACE_DATA_DIR=$DATA_DIR ${ENVARGS[*]:-} $BINARY serve" | tee "$META"
  env OUTPACE_BIND="$HOST:$PORT" \
      OUTPACE_RTMP_BIND="127.0.0.1:$RTMP_PORT" \
      OUTPACE_PEER_LISTEN="0.0.0.0:$PEER_PORT" \
      OUTPACE_DATA_DIR="$DATA_DIR" "${ENVARGS[@]}" \
    "$BINARY" serve >"$DAEMON_LOG" 2>&1 &
  DAEMON_PID=$!
  TARGET_PID="$DAEMON_PID"
  # wait for health
  for _ in $(seq 1 100); do
    if curl -fsS "$BASE_URL/healthz" >/dev/null 2>&1; then break; fi
    kill -0 "$DAEMON_PID" 2>/dev/null || { echo "daemon exited early; see $DAEMON_LOG" >&2; exit 1; }
    sleep 0.2
  done
  curl -fsS "$BASE_URL/healthz" >/dev/null 2>&1 || { echo "daemon never healthy" >&2; exit 1; }
fi

{
  echo "mode=$MODE ids=$IDS duration=$DURATION interval=$INTERVAL transport=$TRANSPORT players=$PLAYERS"
  echo "target_pid=$TARGET_PID base_url=$BASE_URL binary=$BINARY"
  echo "env=$ENVOVERRIDES"
  echo "started=$(date -Is)"
} >>"$META"

IFS=',' read -ra ID_ARR <<<"$IDS"

start_player() {  # start_player <id>
  local id="$1" ext="ts"
  [[ "$TRANSPORT" == "m3u8" ]] && ext="m3u8"
  local url="$BASE_URL/streams/ace/$id.$ext"
  local prog="$OUTDIR/progress-$id.txt"
  : >"$prog"
  ffmpeg -nostdin -hide_banner -loglevel error \
    -i "$url" -map 0 -f null - -progress "$prog" -y \
    >"$OUTDIR/ffmpeg-$id.log" 2>&1 &
  echo $!
}

# ---- start players ----------------------------------------------------------
declare -A PLAYER_PID
if [[ "$MODE" == "play" ]]; then
  for id in "${ID_ARR[@]}"; do
    [[ -z "$id" ]] && continue
    for _ in $(seq 1 "$PLAYERS"); do
      pid=$(start_player "$id"); PIDS_TO_KILL+=("$pid"); PLAYER_PID[$id]=$pid
    done
  done
fi

frames_for() {  # frames_for <id> -> last frame= from progress file
  local prog="$OUTDIR/progress-$1.txt"
  [[ -f "$prog" ]] || { echo 0; return; }
  awk -F= '/^frame=/{f=$2} END{print (f==""?0:f)}' "$prog" 2>/dev/null || echo 0
}

# ---- sample loop ------------------------------------------------------------
START=$(date +%s)
CHURN_NEXT=$((START + CHURN_HOLD)); CHURN_IDX=0
END=$((START + DURATION))
while :; do
  NOW=$(date +%s)
  [[ $NOW -ge $END ]] && break
  ELAPSED=$((NOW - START))

  # process memory
  RSS=$(awk '/^VmRSS:/{print $2}' /proc/$TARGET_PID/status 2>/dev/null || echo "")
  HWM=$(awk '/^VmHWM:/{print $2}' /proc/$TARGET_PID/status 2>/dev/null || echo "")
  THREADS=$(awk '/^Threads:/{print $2}' /proc/$TARGET_PID/status 2>/dev/null || echo "")
  PSS=$(awk '/^Pss:/{print $2}' /proc/$TARGET_PID/smaps_rollup 2>/dev/null || echo "")
  FDS=$(ls /proc/$TARGET_PID/fd 2>/dev/null | wc -l || echo "")

  MSTATS=$(curl -fsS "$BASE_URL/debug/memstats" 2>/dev/null || echo "{}")
  MA=$(echo "$MSTATS" | jq -r '.allocated // ""' 2>/dev/null || echo "")
  MR=$(echo "$MSTATS" | jq -r '.resident // ""' 2>/dev/null || echo "")
  MRT=$(echo "$MSTATS" | jq -r '.retained // ""' 2>/dev/null || echo "")

  echo "$NOW,$ELAPSED,$RSS,$HWM,$PSS,$THREADS,$FDS,$MA,$MR,$MRT" >>"$PROC_CSV"

  # per-stream status
  for id in "${ID_ARR[@]}"; do
    [[ -z "$id" ]] && continue
    body=$(curl -fsS -w '\n%{http_code}' "$BASE_URL/streams/ace/$id/status" 2>/dev/null || printf '\n000')
    code=$(echo "$body" | tail -n1)
    json=$(echo "$body" | sed '$d')
    if [[ "$code" == "200" ]]; then
      cl=$(echo "$json" | jq -r '.clients // ""'); pe=$(echo "$json" | jq -r '.peers // ""')
      br=$(echo "$json" | jq -r '.bitrate // ""'); bm=$(echo "$json" | jq -r '.buffer_ms // ""')
      up=$(echo "$json" | jq -r '.uploaded // ""')
    else cl=""; pe=""; br=""; bm=""; up=""; fi
    fr=$(frames_for "$id")
    echo "$NOW,$ELAPSED,$id,$code,$cl,$pe,$br,$bm,$up,$fr" >>"$STREAM_CSV"
  done

  # churn mode: cycle the active stream
  if [[ "$MODE" == "churn" && $NOW -ge $CHURN_NEXT ]]; then
    for id in "${!PLAYER_PID[@]}"; do kill "${PLAYER_PID[$id]}" 2>/dev/null || true; done
    curl -fsS -X DELETE "$BASE_URL/streams/ace/${ID_ARR[$CHURN_IDX]}" >/dev/null 2>&1 || true
    PLAYER_PID=()
    CHURN_IDX=$(((CHURN_IDX + 1) % ${#ID_ARR[@]}))
    nid="${ID_ARR[$CHURN_IDX]}"
    pid=$(start_player "$nid"); PIDS_TO_KILL+=("$pid"); PLAYER_PID[$nid]=$pid
    CHURN_NEXT=$((NOW + CHURN_HOLD))
  fi

  sleep "$INTERVAL"
done

echo "finished=$(date -Is)" >>"$META"
echo "results in: $OUTDIR"
python3 "$(dirname "${BASH_SOURCE[0]}")/slope.py" "$OUTDIR" || true
