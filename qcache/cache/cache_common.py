
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


class DeleteResult(object):
    STATUS_SUCCESS = "success"

    def __init__(self, status):
        self.status = status
