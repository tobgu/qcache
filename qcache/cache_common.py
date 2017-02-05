import json


class UTF8JSONDecoder(json.JSONDecoder):
    def decode(self, json_string):
        obj = super(UTF8JSONDecoder, self).decode(json_string)
        assert isinstance(obj, list), "Must pass a list of objects"

        for r in obj:
            yield {k: v.encode(encoding='utf-8') if isinstance(v, unicode) else v for k, v in r.items()}


class QueryResult(object):
    STATUS_SUCCESS = "success"
    STATUS_NOT_FOUND = "not_found"
    STATUS_MALFORMED_QUERY = "malformed_query"

    def __init__(self, status, data="", unsliced_length=0, content_type="", query_stats=None):
        if query_stats is None:
            query_stats = {}
        self.status = status
        self.data = data
        self.unsliced_length = unsliced_length
        self.content_type = content_type
        self.query_stats = query_stats


class InsertResult(object):
    STATUS_SUCCESS = "success"

    def __init__(self, status, insert_stats=None):
        if insert_stats is None:
            insert_stats = {}

        self.status = status
        self.insert_stats = insert_stats