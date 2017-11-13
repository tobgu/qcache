import gzip
from io import BytesIO

import lz4 as lz4
from tornado.web import OutputTransform, HTTPError


GZIP_LEVEL = 6

def gzip_dumps(string):
    buffer = BytesIO()
    file = gzip.GzipFile(mode='w', fileobj=buffer, compresslevel=GZIP_LEVEL)
    file.write(string)
    file.close()
    return buffer.getvalue()


def gzip_loads(string):
    buffer = BytesIO(string)
    file = gzip.GzipFile(mode='r', fileobj=buffer)
    return file.read()


ENCODINGS = {
    'lz4': (lz4.block.decompress, lz4.block.compress),
    'gzip': (gzip_loads, gzip_dumps),
    None: (lambda c: c, lambda c: c)
}


def decoded_body(request):
    encoding = request.headers.get('Content-Encoding')
    if encoding not in ENCODINGS:
        raise HTTPError(400,
                        'Unrecognized encoding "{encoding}"'.format(encoding=encoding))

    return ENCODINGS[encoding][0](request.body)


class CompressedContentEncoding(OutputTransform):
    """Applies compression to response. Prefers lz4 if accepted else uses gzip.
    """
    def __init__(self, request):
        accept_coding = request.headers.get("Accept-Encoding", "")
        if 'lz4' in accept_coding:
            self.encoding = 'lz4'
        elif 'gzip' in accept_coding:
            self.encoding = 'gzip'
        else:
            self.encoding = None

        super(CompressedContentEncoding, self).__init__(request)

    def transform_first_chunk(self, status_code, headers, chunk, finishing):
        if status_code != 200:
            # Only compress responses containing query data
            self.encoding = None

        if self.encoding:
            if not finishing:
                raise Exception("Multi chunk not accepted by QCache when applying compression")

            chunk = ENCODINGS[self.encoding][1](chunk)
            headers['Content-Encoding'] = self.encoding
            headers['Content-Length'] = str(len(chunk))

        return status_code, headers, chunk

    def transform_chunk(self, chunk, finishing):
        return chunk
