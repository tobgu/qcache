class MalformedQueryException(Exception):
    pass


def raise_malformed(message, q):
    raise MalformedQueryException(message + ': {q}'.format(q=q))


def assert_integer(name, i):
    if not isinstance(i, (int, long)):
        raise_malformed('Invalid type for {name}'.format(name=name), i)


def assert_list(name, l):
    if not isinstance(l, list):
        raise_malformed('Invalid format for {name}'.format(name=name), l)
