"""
Locust load test behaviour

NB! Locust requires Python 2.7, while qcache runs on Python 3.5+
"""
import json
import random

from locust import HttpLocust, TaskSet, task


def read_csv_file(file_name):
    with open(file_name, 'r') as f:
        return f.read()


def add_data(data_dict, source, count):
    csv_data = read_csv_file('{}.csv'.format(source))
    for i in range(count):
        data_dict['{}_{}'.format(source, i)] = csv_data


class DataContainer(object):
    def __init__(self, small_count=40, medium_count=50, large_count=0):
        self.data = {}
        add_data(self.data, source='small_500', count=small_count)
        add_data(self.data, source='medium_50000', count=medium_count)
        add_data(self.data, source='large_500000', count=large_count)

    def get_key(self):
        return random.choice(self.data.keys())

    def get_data(self, key):
        return self.data[key]

config_1 = dict(small_count=40, medium_count=0, large_count=0)
config_2 = dict(small_count=40, medium_count=50, large_count=0)
config_3 = dict(small_count=250, medium_count=250, large_count=0)

data_container = DataContainer(**config_3)


class CacheClientBehavior(TaskSet):
    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        print("Start CacheClientBehavior called")

    @task(1)
    def small_query(self):
        self.client.get("/qcache/dataset/{}".format(data_container.get_key()),
                        params={'q': json.dumps(dict(limit=200))},
                        headers={'Accept': 'application/json'})

    @task(1)
    def large_query(self):
        self.client.get("/qcache/dataset/{}".format(data_container.get_key()),
                        params={'q': json.dumps(dict(limit=50000))},
                        headers={'Accept': 'application/json'})

    @task(1)
    def insert(self):
        key = data_container.get_key()
        data = data_container.get_data(key)
        self.client.post("/qcache/dataset/{}".format(key),
                         data=data,
                         headers={'content-type': 'text/csv'})


class CacheUser(HttpLocust):
    task_set = CacheClientBehavior
    min_wait = 500
    max_wait = 1000
