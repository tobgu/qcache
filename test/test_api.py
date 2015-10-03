from tornado.testing import AsyncHTTPTestCase
import qcache.app as main

class TestHelloApp(AsyncHTTPTestCase):
    def get_app(self):
        return main.make_app()

    def test_homepage(self):
        response = self.fetch('/foo/abc')
        self.assertEqual(response.code, 404)
#        self.assertEqual(response.body, 'Hello, world')