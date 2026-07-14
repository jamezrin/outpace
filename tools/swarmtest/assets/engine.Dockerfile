# AceStream engine sandbox for the swarmtest interop harness.
#
# Build context MUST be the extracted engine directory (the tarball root that ships
# `start-engine`, `acestreamengine`, `lib/`, and `requirements.txt`). swarmtest passes
# that directory automatically from its engine cache:
#
#   docker build -t swarmtest-engine:latest \
#     -f tools/swarmtest/assets/engine.Dockerfile <extracted-engine-dir>
#
# The engine is closed-source and NEVER committed; it is downloaded/extracted by
# `swarmtest`'s engine module. This image only wraps whatever is in the build context.
#
# Default CMD runs the engine as a CONSUMER (client-console HTTP API on :6878). The
# harness overrides `command:` for the source node with `--stream-source-node ...`.

FROM python:3.10-slim-bookworm

# Break-system-packages so pip installs the engine's wheels into the base env; unbuffered
# so `--log-stderr` shows up promptly in `docker compose logs`.
ENV PIP_BREAK_SYSTEM_PACKAGES=1 \
    PYTHONUNBUFFERED=1

# Build deps for the engine's native python wheels (pycryptodome, lxml, apsw, pynacl):
# build-essential + libffi/libxml2/libxslt headers. ca-certificates + procps for runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libffi-dev \
        libxml2-dev \
        libxslt1-dev \
        ca-certificates \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The whole extracted engine tree (build context) lands at /app, matching the RE sandbox
# layout where `start-engine` and `acestreamengine` live at /app.
COPY . /app/

# Engine python requirements ship in the tarball. Do NOT install frida (RE-only).
RUN pip install --no-cache-dir -r /app/requirements.txt

# `start-engine` is a shell launcher that sets LD_LIBRARY_PATH to the bundled lib/ and
# execs the engine; ensure it is executable after COPY.
RUN chmod +x /app/start-engine

# HTTP API (consumer) + default source-node peer port.
EXPOSE 6878 8621 7764

# Consumer default: client-console exposes the documented HTTP API on :6878.
CMD ["/app/start-engine", "--client-console", "--bind-all", \
     "--log-stderr", "--log-stderr-level", "debug"]
