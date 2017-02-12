from bisect import bisect
import hashlib
from math import ceil
import sys

if sys.version_info[0] >= 3:
    _ord = lambda x: x
else:
    _ord = ord


class NodeRing(object):
    def __init__(self, nodes, weights=None, virtual_count=None):
        assert nodes

        self.ring = {}
        self.sorted_keys = []
        self.weights = weights or {}

        # If number of virtual nodes per real node is not given aim for 1000 nodes in
        # total. That will provide a fairly decent distribution without too much overhead
        # when creating the circle or adding/removing nodes.
        self.virtual_count = virtual_count if virtual_count else int(ceil(1000.0 / len(nodes)))
        self.add_nodes(nodes)
        self.all_nodes = nodes

    def add_node(self, node, weight=None):
        if weight:
            self.weights[node] = weight

        self.add_nodes([node])

    def remove_node(self, node):
        node_keys = set(self.keys_for_node(node))
        self.sorted_keys = [key for key in self.sorted_keys if key not in node_keys]
        for node_key in node_keys:
            del self.ring[node_key]

        self.weights.pop(node, None)

    def keys_for_node(self, node):
        return [generate_key("{node}-{i}".format(node=node, i=i))
                for i in range(self.weights.get(node, 1) * self.virtual_count)]

    def add_nodes(self, nodes):
        for node in nodes:
            for key in self.keys_for_node(node):
                self.ring[key] = node
                self.sorted_keys.append(key)

        self.sorted_keys.sort()

    def get_node(self, string_key):
        if not self.sorted_keys:
            return None

        key = generate_key(string_key)
        pos = bisect(self.sorted_keys, key)
        pos %= len(self.sorted_keys)
        return self.ring[self.sorted_keys[pos]]


def hash_digest(key):
    m = hashlib.md5()
    m.update(bytes(key.encode('utf-8')))
    return [_ord(b) for b in m.digest()]


def generate_key(key):
    byte_key = hash_digest(key)
    return (byte_key[3] << 24) | (byte_key[2] << 16) | (byte_key[1] << 8) | byte_key[0]