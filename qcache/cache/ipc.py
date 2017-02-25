import pickle

import blosc
import time

import zmq

STOP_COMMAND = "stop"
STATUS_OK = "OK"

# TODO: Handle this: zmq.error.ZMQError: Operation cannot be accomplished in current state

# TODO: Split this class to one for shards and one for l2 cache
#       or use multipart messages everywhere...
class ProcessHandle(object):
    def __init__(self, process, ipc_address):
        self.process = process
        self.ipc_address = ipc_address
        self.socket = None

    def stop(self):
        self._ensure_socket()
        send_object(self.socket, STOP_COMMAND)
        self.process.join()

    def _ensure_socket(self):
        if self.socket is None:
            self.socket = zmq.Context.instance().socket(zmq.REQ)
            self.socket.connect(self.ipc_address)

    def send_object(self, obj):
        self._ensure_socket()
        return send_object(self.socket, obj)

    def send_serialized_object(self, obj):
        self._ensure_socket()
        return send_serialized_object(self.socket, obj)

    def send_objects(self, *objects):
        return self.send_serialized_objects(*[serialize_object(o) for o in objects])

    def send_serialized_objects(self, *serialized_objs):
        self._ensure_socket()
        return send_serialized_objects(self.socket, *serialized_objs)

    def receive_object(self):
        self._ensure_socket()
        return receive_object(self.socket)

    def receive_serialized_objects(self):
        self._ensure_socket()
        return receive_serialized_objects(self.socket)

    def receive_objects(self):
        self._ensure_socket()
        return receive_objects(self.socket)


def serialize_object(obj):
        serialized_object = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        return blosc.compress(serialized_object, typesize=1, cname='lz4')


def deserialize_object(buffer):
    serialized_obj = blosc.decompress(buffer)
    obj = pickle.loads(serialized_obj)
    if isinstance(obj, Exception):
        raise obj
    return obj


def send_object(socket, obj):
    serialized = serialize_object(obj)
    return send_serialized_object(socket, serialized)


def send_serialized_object(socket, serialized_obj):
    if serialized_obj is None:
        raise Exception("None not allowed!")

    socket.send(serialized_obj, copy=False)
    return serialized_obj


def send_serialized_objects(socket, *serialized_objs):
    socket.send_multipart(serialized_objs, copy=False)
    return serialized_objs


def send_objects(socket, *objs):
    socket.send_multipart([serialize_object(o) for o in objs], copy=False)
    return objs


def receive_object(socket):
    msg = socket.recv(copy=False)
    t0 = time.time()
    obj = deserialize_object(msg.buffer)
    return obj, t0


def receive_serialized_objects(socket):
    return [msg.buffer for msg in socket.recv_multipart(copy=False)]


def receive_objects(socket):
    return [deserialize_object(o) for o in receive_serialized_objects(socket)]