#!/usr/bin/env bash
# acestream_probe — A/B baseline: run the reference Acestream engine in docker,
# play the same content ids with real decoders, sample container memory over time.
# Emits process.csv/streams.csv in the same schema as run.sh so slope.py works.
set -euo pipefail

IDS=""
DURATION=300
INTERVAL=5
HOST_PORT=16878          # host port mapped to container 6878
CONTAINER="aceserve-memsoak"
IMAGE="jopsis/aceserve:latest"
MEM_CACHE=104857600       # match image default (100 MiB) for a fair baseline
OUTDIR=""
LABEL="acestream"
KEEP=0                    # 1 => leave container running on exit

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ids) IDS="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --interval) INTERVAL="$2"; shift 2;;
    --host-port) HOST_PORT="$2"; shift 2;;
    --mem-cache) MEM_CACHE="$2"; shift 2;;
    --out) OUTDIR="$2"; shift 2;;
    --label) LABEL="$2"; shift 2;;
    --keep) KEEP=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_URL="http://127.0.0.1:$HOST_PORT"
[[ -z "$OUTDIR" ]] && OUTDIR="$REPO_ROOT/tools/memsoak/results/$(date +%Y%m%d-%H%M%S)-$LABEL"
mkdir -p "$OUTDIR"
PROC_CSV="$OUTDIR/process.csv"; STREAM_CSV="$OUTDIR/streams.csv"; META="$OUTDIR/meta.txt"
echo "ts_unix,elapsed_s,rss_kb,hwm_kb,pss_kb,threads,fds,mem_allocated,mem_resident,mem_retained" >"$PROC_CSV"
echo "ts_unix,elapsed_s,id,http_code,clients,peers,bitrate,buffer_ms,uploaded,frames" >"$STREAM_CSV"

PLAYER_PIDS=()
cleanup() {
  set +e
  for p in "${PLAYER_PIDS[@]:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null; done
  if [[ "$KEEP" == "0" ]]; then docker rm -f "$CONTAINER" >/dev/null 2>&1; fi
}
trap cleanup EXIT INT TERM

# ---- start container --------------------------------------------------------
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
echo "starting $IMAGE as $CONTAINER (mem-cache=$MEM_CACHE) on host port $HOST_PORT" | tee "$META"
docker run -d --name "$CONTAINER" -p "$HOST_PORT:6878" --dns 1.1.1.1 "$IMAGE" \
  python main.py --bind-all --live-cache-type memory --live-mem-cache-size "$MEM_CACHE" \
  --disable-sentry --log-stdout --disable-upnp >/dev/null

# wait for the HTTP API
for _ in $(seq 1 120); do
  curl -fsS "$BASE_URL/webui/api/service?method=get_version&format=json" >/dev/null 2>&1 && break
  curl -fsS "$BASE_URL/ace/getstream" >/dev/null 2>&1 && break
  docker ps --format '{{.Names}}' | grep -q "^$CONTAINER$" || { echo "container died"; docker logs "$CONTAINER" | tail -20; exit 1; }
  sleep 1
done

IFS=',' read -ra ID_ARR <<<"$IDS"
declare -A STAT_URL PROG

start_player() {  # start_player <id>
  local id="$1"
  local resp playback stat
  resp=$(curl -fsS "$BASE_URL/ace/getstream?id=$id&format=json" 2>/dev/null || echo '{}')
  playback=$(echo "$resp" | jq -r '.response.playback_url // empty' 2>/dev/null)
  stat=$(echo "$resp" | jq -r '.response.stat_url // empty' 2>/dev/null)
  [[ -z "$playback" ]] && { echo "no playback_url for $id: $resp" >&2; return 1; }
  STAT_URL[$id]="$stat"
  local prog="$OUTDIR/progress-$id.txt"; : >"$prog"; PROG[$id]="$prog"
  ffmpeg -nostdin -hide_banner -loglevel error -i "$playback" -map 0 -f null - \
    -progress "$prog" -y >"$OUTDIR/ffmpeg-$id.log" 2>&1 &
  PLAYER_PIDS+=($!)
}

for id in "${ID_ARR[@]}"; do [[ -n "$id" ]] && start_player "$id" || true; done

frames_for() { awk -F= '/^frame=/{f=$2} END{print (f==""?0:f)}' "${PROG[$1]:-/dev/null}" 2>/dev/null || echo 0; }

mem_kb() {  # container ANONYMOUS memory in kB (all processes), for a fair heap-to-heap A/B.
  # `anon` from cgroup memory.stat is anonymous (heap/stack) pages only — it excludes the
  # reclaimable file-backed page cache that Acestream's on-disk HLS segments would otherwise
  # inflate. This is the counterpart of outpace's RssAnon.
  local cid
  cid=$(docker inspect -f '{{.Id}}' "$CONTAINER" 2>/dev/null) || { echo ""; return; }
  for base in /sys/fs/cgroup/system.slice/docker-$cid.scope /sys/fs/cgroup/docker/$cid; do
    if [[ -f "$base/memory.stat" ]]; then
      awk '/^anon /{print int($2/1024); found=1} END{if(!found) print ""}' "$base/memory.stat" 2>/dev/null
      return
    fi
  done
  # fallback: docker stats working set (includes some file cache)
  docker stats --no-stream --format '{{.MemUsage}}' "$CONTAINER" 2>/dev/null \
    | awk '{print $1}' | sed 's/MiB//;s/GiB/*1024/' | bc 2>/dev/null | awk '{print int($1*1024)}'
}

START=$(date +%s); END=$((START+DURATION))
{ echo "ids=$IDS duration=$DURATION mem_cache=$MEM_CACHE"; echo "started=$(date -Is)"; } >>"$META"
while :; do
  NOW=$(date +%s); [[ $NOW -ge $END ]] && break
  EL=$((NOW-START))
  RSS=$(mem_kb)
  echo "$NOW,$EL,$RSS,,,,,,," >>"$PROC_CSV"
  for id in "${ID_ARR[@]}"; do
    [[ -z "$id" ]] && continue
    peers=""; code="200"
    if [[ -n "${STAT_URL[$id]:-}" ]]; then
      j=$(curl -fsS "${STAT_URL[$id]}" 2>/dev/null || echo '{}')
      peers=$(echo "$j" | jq -r '.response.peers // ""' 2>/dev/null)
    fi
    echo "$NOW,$EL,$id,$code,,$peers,,,,$(frames_for "$id")" >>"$STREAM_CSV"
  done
  sleep "$INTERVAL"
done
echo "finished=$(date -Is)" >>"$META"
echo "results in: $OUTDIR"
python3 "$(dirname "${BASH_SOURCE[0]}")/slope.py" "$OUTDIR" || true
