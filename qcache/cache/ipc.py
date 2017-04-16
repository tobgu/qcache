"""
Helpers for Process and Inter Process Communication (IPC).
"""
from typing import Any, List, Tuple

import os
import pickle

import blosc
import time

import zmq
from multiprocessing import Process

STOP_COMMAND: str = "stop"
STATUS_OK: str = "OK"


class ProcessHandle:
    def __init__(self, process: Process, ipc_address: str) -> None:
        self.process = process
        self.ipc_address = ipc_address
        self.socket: zmq.Socket = None

    def stop(self):
        self._ensure_socket()
        send_object(self.socket, STOP_COMMAND)
        self.process.join()

    def is_alive(self) -> bool:
        try:
            # Seems like mypy cannot figure out that there is a pid on the Process
            os.kill(self.process.pid, 0)  # type: ignore
        except OSError:
            return False

        return True

    def _ensure_socket(self):
        if self.socket is None:
            self.socket = zmq.Context.instance().socket(zmq.REQ)
            self.socket.connect(self.ipc_address)

    def send_object(self, obj: Any):
        self._ensure_socket()
        return send_object(self.socket, obj)

    def send_serialized_object(self, obj: Any):
        self._ensure_socket()
        return send_serialized_object(self.socket, obj)

    def send_objects(self, *objects: Any) -> Tuple[bytes, ...]:
        return self.send_serialized_objects(*[serialize_object(o) for o in objects])

    def send_serialized_objects(self, *serialized_objs: bytes):
        self._ensure_socket()
        return send_serialized_objects(self.socket, *serialized_objs)

    def receive_object(self) -> Tuple[Any, float]:
        self._ensure_socket()
        return receive_object(self.socket)

    def receive_serialized_objects(self) -> List[bytes]:
        self._ensure_socket()
        return receive_serialized_objects(self.socket)

    def receive_objects(self) -> List[Any]:
        self._ensure_socket()
        return receive_objects(self.socket)


def serialize_object(obj: Any) -> bytes:
        serialized_object = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        return blosc.compress(serialized_object, typesize=1, cname='lz4')


def deserialize_object(buffer: bytes) -> Any:
    serialized_obj = blosc.decompress(buffer)
    obj = pickle.loads(serialized_obj)
    if isinstance(obj, Exception):
        raise obj
    return obj


def send_object(socket: zmq.Socket, obj: Any) -> bytes:
    serialized = serialize_object(obj)
    return send_serialized_object(socket, serialized)


def send_serialized_object(socket: zmq.Socket, serialized_obj: bytes) -> bytes:
    if serialized_obj is None:
        raise Exception("None not allowed!")

    socket.send(serialized_obj, copy=False)
    return serialized_obj


def send_serialized_objects(socket: zmq.Socket, *serialized_objs: bytes) -> Tuple[bytes, ...]:
    socket.send_multipart(serialized_objs, copy=False)
    return serialized_objs


def send_objects(socket: zmq.Socket, *objs: Any) -> Tuple[Any, ...]:
    socket.send_multipart([serialize_object(o) for o in objs], copy=False)
    return objs


def receive_object(socket: zmq.Socket) -> Tuple[Any, float]:
    msg = socket.recv(copy=False)
    t0 = time.time()
    obj = deserialize_object(msg.buffer)
    return obj, t0


def receive_serialized_objects(socket: zmq.Socket) -> List[bytes]:
    return [msg.buffer for msg in socket.recv_multipart(copy=False)]


def receive_objects(socket: zmq.Socket) -> List[Any]:
    return [deserialize_object(o) for o in receive_serialized_objects(socket)]
