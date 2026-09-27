#!/usr/bin/env python3
"""Loopback-only CONNECT relay for the Mac-routed DeepSeek endpoint."""

import logging
import re
import select
import socket
import socketserver


LOGGER = logging.getLogger("deepseek_connect_proxy")
HEADER_LIMIT = 16_384
HEADER_TIMEOUT = 5
CONNECT_TIMEOUT = 10
REQUEST_LINE = b"CONNECT api.deepseek.com:443 HTTP/1.1"
HEADER_NAME = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+")


def read_request(client):
    """Read exactly one bounded CONNECT header block and reject early payloads."""
    client.settimeout(HEADER_TIMEOUT)
    data = bytearray()
    while b"\r\n\r\n" not in data:
        remaining = HEADER_LIMIT - len(data)
        if remaining <= 0:
            return False
        chunk = client.recv(min(4096, remaining))
        if not chunk:
            return False
        data.extend(chunk)

    header, extra = bytes(data).split(b"\r\n\r\n", 1)
    if extra or len(data) > HEADER_LIMIT:
        return False
    lines = header.split(b"\r\n")
    if not lines or lines[0] != REQUEST_LINE:
        return False
    seen_host = False
    for line in lines[1:]:
        if b":" not in line:
            return False
        name, value = line.split(b":", 1)
        if not HEADER_NAME.fullmatch(name) or any(byte < 32 and byte != 9 for byte in value):
            return False
        lowered = name.lower()
        if lowered in {b"content-length", b"transfer-encoding"}:
            return False
        if lowered == b"host":
            if seen_host or value.strip() != b"api.deepseek.com:443":
                return False
            seen_host = True
    return seen_host


def open_upstream(host, port):
    return socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)


def relay(client, upstream):
    """Forward bytes both ways until each side has closed its write half."""
    client.settimeout(None)
    upstream.settimeout(None)
    readable = {client: upstream, upstream: client}
    while readable:
        ready, _, _ = select.select(list(readable), [], [])
        for source in ready:
            if source not in readable:
                continue
            destination = readable[source]
            chunk = source.recv(65536)
            if chunk:
                destination.sendall(chunk)
            else:
                del readable[source]
                destination.shutdown(socket.SHUT_WR)


def handle_connection(client, connector=open_upstream):
    try:
        with client:
            try:
                if not read_request(client):
                    LOGGER.info("connection denied")
                    client.sendall(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
                    return
                upstream = connector("api.deepseek.com", 443)
            except (OSError, TimeoutError):
                LOGGER.info("connection failed")
                try:
                    client.sendall(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
                except OSError:
                    pass
                return
            with upstream:
                LOGGER.info("connection allowed")
                client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                relay(client, upstream)
    except OSError:
        LOGGER.info("connection failed")


class ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        handle_connection(self.request)


class ProxyServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        LOGGER.info("connection failed")


def create_server():
    return ProxyServer(("127.0.0.1", 18081), ProxyHandler)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    with create_server() as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
