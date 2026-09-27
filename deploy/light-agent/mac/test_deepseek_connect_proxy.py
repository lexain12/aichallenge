"""Local, credential-free contract tests for the Mac CONNECT proxy."""

import importlib.util
import io
import logging
from pathlib import Path
import socket
import threading
import unittest
from contextlib import redirect_stderr
from unittest import mock


PROXY_PATH = Path(__file__).with_name("deepseek_connect_proxy.py")


def load_proxy():
    if not PROXY_PATH.is_file():
        raise AssertionError("Mac CONNECT proxy implementation is missing")
    spec = importlib.util.spec_from_file_location("deepseek_connect_proxy", PROXY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProxyTests(unittest.TestCase):
    def setUp(self):
        self.proxy = load_proxy()

    def run_exchange(self, request, upstream=None):
        client, proxy_side = socket.socketpair()
        peer = None
        if upstream is not None:
            upstream_socket, peer = socket.socketpair()
            connector = mock.Mock(return_value=upstream_socket)
        else:
            connector = mock.Mock(side_effect=AssertionError("upstream opened"))
        worker = threading.Thread(
            target=self.proxy.handle_connection,
            args=(proxy_side, connector),
            daemon=True,
        )
        worker.start()
        client.settimeout(2)
        if peer is not None:
            peer.settimeout(2)
        try:
            client.sendall(request)
        except BrokenPipeError:
            pass  # An oversized request can be rejected before sendall completes.
        return client, peer, connector, worker

    def test_connect_relays_both_directions_and_half_closes(self):
        client, upstream, connector, worker = self.run_exchange(
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\n"
            b"Host: api.deepseek.com:443\r\n\r\n",
            upstream=True,
        )
        try:
            self.assertEqual(client.recv(4096), b"HTTP/1.1 200 Connection Established\r\n\r\n")
            connector.assert_called_once_with("api.deepseek.com", 443)
            client.sendall(b"client bytes")
            self.assertEqual(upstream.recv(4096), b"client bytes")
            client.shutdown(socket.SHUT_WR)
            self.assertEqual(upstream.recv(1), b"")
            upstream.sendall(b"server bytes")
            upstream.shutdown(socket.SHUT_WR)
            self.assertEqual(client.recv(4096), b"server bytes")
            self.assertEqual(client.recv(1), b"")
            worker.join(2)
            self.assertFalse(worker.is_alive())
        finally:
            client.close()
            upstream.close()

    def test_rejected_methods_and_authorities_never_open_upstream(self):
        for request in [
            b"GET api.deepseek.com:443 HTTP/1.1\r\n\r\n",
            b"CONNECT example.com:443 HTTP/1.1\r\n\r\n",
            b"CONNECT api.deepseek.com:80 HTTP/1.1\r\n\r\n",
            b"CONNECT api.deepseek.com:443 HTTP/1.0\r\n\r\n",
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\n\r\n",
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\nHost: other.example\r\n\r\n",
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\nHost: api.deepseek.com:443\r\nHost: api.deepseek.com:443\r\n\r\n",
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\nContent-Length: 1\r\n\r\nx",
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\n\r\nearly-data",
        ]:
            with self.subTest(request=request.split(b"\r\n", 1)[0]):
                client, _, connector, worker = self.run_exchange(request)
                try:
                    self.assertTrue(client.recv(128).startswith(b"HTTP/1.1 400 "))
                    worker.join(2)
                    self.assertFalse(worker.is_alive())
                    connector.assert_not_called()
                finally:
                    client.close()

    def test_oversized_or_incomplete_headers_are_rejected_before_upstream(self):
        for request in [
            b"CONNECT api.deepseek.com:443 HTTP/1.1\r\nX-Fill: " + b"a" * 17000 + b"\r\n\r\n",
            b"CONNECT api.deepseek.com:443 HTTP/1.1\n\n",
        ]:
            client, _, connector, worker = self.run_exchange(request)
            try:
                try:
                    client.shutdown(socket.SHUT_WR)
                except OSError:
                    pass  # An oversized header can be rejected before this call.
                self.assertTrue(client.recv(128).startswith(b"HTTP/1.1 400 "))
                worker.join(2)
                self.assertFalse(worker.is_alive())
                connector.assert_not_called()
            finally:
                client.close()

    def test_server_binds_to_fixed_mac_loopback(self):
        with mock.patch.object(self.proxy, "ProxyServer") as server:
            self.proxy.create_server()
        server.assert_called_once_with(("127.0.0.1", 18081), self.proxy.ProxyHandler)

    def test_logs_only_generic_connection_status(self):
        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        logger = self.proxy.LOGGER
        capture = Capture()
        previous_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(capture)
        try:
            client, _, _, worker = self.run_exchange(
                b"CONNECT secret.example:443 HTTP/1.1\r\nX-Private: hidden\r\n\r\n"
            )
            client.recv(128)
            worker.join(2)
            client.close()
        finally:
            logger.removeHandler(capture)
            logger.setLevel(previous_level)
        self.assertTrue(records)
        self.assertTrue(all(message in {"connection allowed", "connection denied", "connection failed"} for message in records))

    def test_unexpected_handler_error_does_not_log_client_address(self):
        output = io.StringIO()
        server = object.__new__(self.proxy.ProxyServer)
        try:
            raise RuntimeError("sensitive request data")
        except RuntimeError:
            with redirect_stderr(output):
                server.handle_error(None, ("private-client", 1234))
        self.assertEqual(output.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
