#!/usr/bin/env python3
"""Loopback-only CONNECT relay for the Mac-routed DeepSeek endpoint."""

import logging
import re
import select
import socket
import socketserver
import threading
import time


LOGGER = logging.getLogger("deepseek_connect_proxy")
HEADER_LIMIT = 16_384
HEADER_TIMEOUT = 5
CONNECT_TIMEOUT = 10
REQUEST_LINE = b"CONNECT api.deepseek.com:443 HTTP/1.1"
HEADER_NAME = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+")


def read_request(client):
    """Read exactly one bounded CONNECT header block and reject early payloads."""
    deadline = time.monotonic() + HEADER_TIMEOUT
    data = bytearray()
    while b"\r\n\r\n" not in data:
        remaining = HEADER_LIMIT - len(data)
        remaining_time = deadline - time.monotonic()
        if remaining <= 0 or remaining_time <= 0:
            return False
        client.settimeout(remaining_time)
        try:
            chunk = client.recv(min(4096, remaining))
        except socket.timeout:
            return False
        if time.monotonic() >= deadline:
            return False
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
    client.setblocking(False)
    upstream.setblocking(False)
    opposite = {client: upstream, upstream: client}
    read_open = {client: True, upstream: True}
    write_open = {client: True, upstream: True}
    pending = {client: bytearray(), upstream: bytearray()}
    buffer_limit = 65536

    while any(read_open.values()) or any(pending.values()):
        readers = [
            source
            for source in opposite
            if read_open[source] and len(pending[opposite[source]]) < buffer_limit
        ]
        writers = [destination for destination in opposite if pending[destination]]
        ready_read, ready_write, _ = select.select(readers, writers, [])

        for source in ready_read:
            destination = opposite[source]
            try:
                chunk = source.recv(buffer_limit - len(pending[destination]))
            except BlockingIOError:
                continue
            if chunk:
                pending[destination].extend(chunk)
            else:
                read_open[source] = False
                if not pending[destination] and write_open[destination]:
                    destination.shutdown(socket.SHUT_WR)
                    write_open[destination] = False

        for destination in ready_write:
            try:
                sent = destination.send(pending[destination])
            except BlockingIOError:
                continue
            if sent == 0:
                raise ConnectionError("relay destination closed")
            del pending[destination][:sent]
            if not pending[destination] and not read_open[opposite[destination]]:
                destination.shutdown(socket.SHUT_WR)
                write_open[destination] = False


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
    MAX_HANDLERS = 32

    def __init__(self, server_address, handler, bind_and_activate=True):
        self._handler_slots = threading.BoundedSemaphore(self.MAX_HANDLERS)
        super().__init__(server_address, handler, bind_and_activate)

    def process_request(self, request, client_address):
        if not self._handler_slots.acquire(blocking=False):
            LOGGER.info("connection denied")
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._handler_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._handler_slots.release()

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
