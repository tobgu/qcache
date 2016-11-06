"""
Context to keep track of the qframe that is currently being operated on.

NB! Not thread safe and not safe for interleaved operations on multiple frames.
"""

_current_qframe = None


def set_current_qframe(qframe):
    global _current_qframe
    _current_qframe = qframe


def get_current_qframe():
    return _current_qframe
