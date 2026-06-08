import os
import logging
import socket
from pathlib import Path
from typing import Optional

import zmq
# import msgpack
import pickle

from connection_ledger import ConnectionLedger

logger = logging.getLogger(__name__)


class FrameCodec:
    @staticmethod
    def encode(frame_dict: dict) -> bytes:
        return pickle.dumps(frame_dict)

    @staticmethod
    def decode(raw: bytes) -> dict:
        return pickle.loads(raw)


class ZmqSessionManager:

    def __init__(self, bind_address: str, port: int):
        self._address = bind_address
        self._port = port
        self._context: Optional[zmq.Context] = None
        self._socket: Optional[zmq.Socket] = None

    @property
    def endpoint(self) -> str:
        return f"tcp://{self._address}:{self._port}"

    def open(self) -> zmq.Socket:
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PAIR)
        self._socket.bind(self.endpoint)
        return self._socket

    def close(self) -> None:
        if self._socket:
            self._socket.close()
            self._socket = None

    def reset(self) -> zmq.Socket:
        self.close()
        return self.open()

    def configure_timeouts(self, send_ms: int, recv_ms: int) -> None:
        if self._socket:
            self._socket.SNDTIMEO = send_ms
            self._socket.RCVTIMEO = recv_ms


class FragmentSender:

    def __init__(self, config, ledger: ConnectionLedger,
                 router_manager=None, codec: Optional[FrameCodec] = None):
        self._config = config
        self._ledger = ledger
        self._router = router_manager
        self._codec = codec or FrameCodec()

    def _emit_start_signal(self, sock: zmq.Socket) -> None:
        payload = self._codec.encode({"type": "start"})
        sock.send(payload)

    def _negotiate_resume_point(self, sock: zmq.Socket, file_handle) -> None:
        raw = sock.recv()
        resume_info = self._codec.decode(raw)
        offset = resume_info["data"]
        file_handle.seek(offset, 0)
        logger.info("Resuming send from byte %d", offset)

    def _transmit_chunks(self, sock: zmq.Socket, file_handle) -> bool:
        chunk = file_handle.read(self._config.chunk_size)

        while chunk:
            try:
                data_frame = self._codec.encode({"type": "data", "data": chunk})
                sock.send(data_frame)

                ack_raw = sock.recv()
                self._codec.decode(ack_raw)

                chunk = file_handle.read(self._config.chunk_size)
            except Exception:
                logger.warning("Connection lost during send")
                return False

        return True

    def _attempt_reconnect(self, session: ZmqSessionManager,
                           file_handle) -> Optional[zmq.Socket]:
        try:
            new_sock = session.reset()
            session.configure_timeouts(
                self._config.disconnected_timeout,
                self._config.disconnected_timeout,
            )
            logger.info("Waiting for peer to reconnect...")
            self._emit_start_signal(new_sock)
            logger.info("Peer reconnected")

            self._negotiate_resume_point(new_sock, file_handle)

            session.configure_timeouts(
                self._config.chunk_timeout,
                self._config.chunk_timeout,
            )
            return new_sock
        except Exception:
            logger.error("Reconnection failed, giving up")
            return None

    def _close_router_port(self, port: int) -> None:
        if not self._config.local and not self._config.hosted and self._router:
            self._router.forward_port(
                port, port, router=None, lanip=None,
                disable=True, protocol="TCP", duration=0,
                description=None, verbose=False,
            )

    def send_data(self, request: dict, is_new_transfer: bool) -> None:
        shard_path = os.path.join(self._config.data_directory, request["shard_id"])
        if not os.path.isfile(shard_path):
            logger.error("Fragment not found: %s", request["shard_id"])
            return

        session = ZmqSessionManager(self._config.local_ip, request["port"])
        sock = session.open()

        self._emit_start_signal(sock)

        fh = open(shard_path, "rb")

        if not is_new_transfer:
            logger.info("Resuming previous send")
            self._negotiate_resume_point(sock, fh)
        else:
            logger.info("Starting fresh send")

        session.configure_timeouts(self._config.chunk_timeout, self._config.chunk_timeout)

        completed = self._transmit_chunks(sock, fh)

        if not completed:
            sock = self._attempt_reconnect(session, fh)
            if sock is not None:
                completed = self._transmit_chunks(sock, fh)

        if completed:
            end_frame = self._codec.encode({"type": "END"})
            sock.send(end_frame) if session._socket else None
            logger.info("Send complete for %s", request["shard_id"])

        fh.close()
        session.close()
        logger.info("Closed port %d", request["port"])

        self._close_router_port(request["port"])
        self._ledger.unregister(request)


class FragmentReceiver:

    def __init__(self, config, ledger: ConnectionLedger,
                 backend_client=None, router_manager=None,
                 codec: Optional[FrameCodec] = None):
        self._config = config
        self._ledger = ledger
        self._backend = backend_client
        self._router = router_manager
        self._codec = codec or FrameCodec()

    def _emit_start_signal(self, sock: zmq.Socket) -> None:
        payload = self._codec.encode({"type": "start"})
        sock.send(payload)

    def _send_resume_offset(self, sock: zmq.Socket, byte_offset: int) -> None:
        frame = self._codec.encode({"type": "resume", "data": byte_offset})
        sock.send(frame)

    def _open_shard_file(self, shard_path: str, sock: zmq.Socket):
        if os.path.isfile(shard_path):
            current_size = os.path.getsize(shard_path)
            self._send_resume_offset(sock, current_size)
            return open(shard_path, "ab")
        else:
            return open(shard_path, "wb")

    def _receive_chunks(self, sock: zmq.Socket, file_handle) -> bool:
        while True:
            try:
                raw = sock.recv()
                frame = self._codec.decode(raw)

                if frame["type"] == "data":
                    ack = self._codec.encode({"type": "ACK"})
                    sock.send(ack)
                    file_handle.write(frame["data"])

                elif frame["type"] == "END":
                    return True

            except Exception:
                logger.warning("Connection lost during receive")
                return False

    def _attempt_reconnect(self, session: ZmqSessionManager,
                           shard_path: str) -> Optional[tuple]:
        try:
            new_sock = session.reset()
            session.configure_timeouts(
                self._config.disconnected_timeout,
                self._config.disconnected_timeout,
            )
            logger.info("Waiting for sender to reconnect...")
            self._emit_start_signal(new_sock)
            logger.info("Sender reconnected")

            session.configure_timeouts(
                self._config.chunk_timeout,
                self._config.chunk_timeout,
            )

            current_size = os.path.getsize(shard_path)
            self._send_resume_offset(new_sock, current_size)
            logger.info("Resuming receive from byte %d", current_size)

            fh = open(shard_path, "ab")
            return new_sock, fh
        except socket.error:
            logger.error("Reconnection failed, giving up")
            return None

    def _close_router_port(self, port: int) -> None:
        if not self._config.local and not self._config.hosted and self._router:
            self._router.forward_port(
                port, port, router=None, lanip=None,
                disable=True, protocol="TCP", duration=0,
                description=None, verbose=False,
            )

    def receive_data(self, request: dict) -> None:
        if not os.path.isdir(self._config.data_directory):
            os.makedirs(self._config.data_directory)

        shard_path = os.path.join(self._config.data_directory, request["shard_id"])

        session = ZmqSessionManager(self._config.local_ip, request["port"])
        sock = session.open()

        self._emit_start_signal(sock)
        fh = self._open_shard_file(shard_path, sock)

        session.configure_timeouts(self._config.chunk_timeout, self._config.chunk_timeout)

        logger.info("Receiving data for %s", request["shard_id"])
        completed = self._receive_chunks(sock, fh)

        if not completed:
            fh.close()
            reconnect_result = self._attempt_reconnect(session, shard_path)
            if reconnect_result is not None:
                new_sock, fh = reconnect_result
                completed = self._receive_chunks(new_sock, fh)

        fh.close()
        session.close()

        if completed:
            logger.info("Receive complete for %s", request["shard_id"])
            if self._backend:
                self._backend.done_uploading(request["shard_id"])

        logger.info("Closed port %d", request["port"])
        self._close_router_port(request["port"])
        self._ledger.unregister(request)