#!/usr/bin/env python3
"""Bounded loopback probe for the deprecated Ace Stream TCP control API."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import os
import re
import socket
import sys


MAX_LINE_BYTES = 4096
KEY_PATTERN = re.compile(r"(?:^| )key=([^ ]+)(?: |$)")


def receive_line(sock: socket.socket) -> str:
    data = bytearray()
    while not data.endswith(b"\r\n"):
        remaining = MAX_LINE_BYTES - len(data)
        if remaining <= 0:
            raise RuntimeError(f"response exceeded {MAX_LINE_BYTES} bytes")
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("connection closed before CRLF")
        data.extend(chunk)
    try:
        return data[:-2].decode("utf-8")
    except UnicodeDecodeError as error:
        raise RuntimeError("response was not valid UTF-8") from error


def require_loopback_literal(host: str) -> None:
    try:
        address = ipaddress.ip_address(host)
    except ValueError as error:
        raise ValueError("--host must be a loopback IP literal") from error
    if not address.is_loopback:
        raise ValueError("refusing to probe a non-loopback address")


def ready_command(hello: str, product_key: str | None) -> str:
    if product_key is None:
        return "READY"

    match = KEY_PATTERN.search(hello)
    if match is None:
        raise RuntimeError("HELLOTS response did not contain a challenge key")
    request_key = match.group(1)
    digest = hashlib.sha1((request_key + product_key).encode()).hexdigest()
    product_id = product_key.split("-", 1)[0]
    return f"READY key={product_id}-{digest}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=62062)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument(
        "--product-key-env",
        default="ACE_OLD_API_PRODUCT_KEY",
        help="environment variable containing a legacy product key",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        require_loopback_literal(args.host)
        if not 1 <= args.port <= 65535:
            raise ValueError("--port must be between 1 and 65535")
        if not 0 < args.timeout <= 10:
            raise ValueError("--timeout must be greater than 0 and at most 10 seconds")

        product_key = os.environ.get(args.product_key_env)
        with socket.create_connection((args.host, args.port), args.timeout) as sock:
            sock.settimeout(args.timeout)
            sock.sendall(b"HELLOBG version=3\r\n")
            hello = receive_line(sock)
            if not hello.startswith("HELLOTS"):
                raise RuntimeError(f"unexpected handshake response: {hello!r}")
            print(f"handshake: {hello}")

            command = ready_command(hello, product_key)
            sock.sendall(command.encode() + b"\r\n")
            result = receive_line(sock)
            mode = "keyed" if product_key is not None else "unkeyed"
            print(f"ready ({mode}): {result}")
            if product_key is None and result == "NOTREADY":
                print(f"hint: set {args.product_key_env} to exercise keyed authentication")
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        print(f"probe failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
