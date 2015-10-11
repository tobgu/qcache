"""
Rough script used to measure insert and query performance + memory usage.

For memory usage estimate ps_mem
(http://github.com/pixelb/scripts/commits/master/scripts/ps_mem.py) was used.

 Results

 Sizes: 1000, 5000, 10000, 20000, 50000, 100000, 200000, 400000 rows
 Cache size: 1 Gb
 Insert only, 1 Gb cache configured => 1,2 Gb used
 Insert followed by 0 - 5 queries against some of the latest 40 inserted datasets
 * 2.2 - 2.5 Gb used
 * Query response time 7 - 55 ms observed
 * Cache eviction 1 - 15 ms, datasets in cache 60 - 100, dropped 1 - 11 at a time

 Sizes: 1000, 5000, 10000, 20000, 50000, 100000, 200000, 400000 rows
 Cache size: 1 Gb
 gc.collect() after every query
 Insert followed by 0 - 5 queries against some of the latest 40 inserted datasets
 * 1.2 - 1.3 Gb used
 * Query response time 22 - 65 ms observed
 * Cache eviction 1 - 15 ms, datasets in cache 60 - 100, dropped 1 - 11 at a time

 Sizes: 1000, 5000, 10000, 20000, 50000, 100000, 200000, 400000 rows
 Cache size: 1 Gb
 gc.collect() after every 10th
 Insert followed by 0 - 5 queries against some of the latest 40 inserted datasets
 * 1.2 - 1.3 Gb used
 * Insert times 600 ms - 850 ms observed
 * Query response time 7 - 70 ms observed
 * Insert times 90 ms - 1150 ms observed
 * Cache eviction 1 - 15 ms, datasets in cache 60 - 100, dropped 1 - 13 at a time

 Sizes: 1000, 5000, 10000, 15000, 20000, 30000, 40000, 50000 rows
 Cache size: 1 Gb
 gc.collect() after every 10th
 Insert followed by 0 - 5 queries against some of the latest 40 inserted datasets
 * 1.2 - 1.3 Gb used
 * Query response time 7 - 70 ms observed
 * Insert times 600 ms - 850 ms observed
 * Cache eviction 1 - 15 ms, datasets in cache 400 - 430, dropped 1 - 13 at a time
 Performance quite similar to other examples with larger datasets.
"""

from StringIO import StringIO
import csv
import json
import random
import string
import requests
import time

example_data_row = {
    'text1': '123abc123', 'text2': 'asdfghjkl', 'some_text': 'aaaaaaaaaaaaaaa', 'a_status': 'b',
    'some_number': 1234567, 'a_float': 1234.1234,
    'a_class': 'qwertyuuer', 'some_label': '1234yzx', 'another_label': '1234yzx',
    'classifier': 'long_classifier', 'another_class': '1', 'float1': 98765432.123,
    'float2': 12345568.9876, 'description': 'a/b/c'}


SELECTION = ['aaaaaaaaaaaaaaaaaaa',
             'bbbbbbbbbbbbbbbbbbb',
             'ccccccccccccccccccc',
             'ddddddddddddddddddd',
             'eeeeeeeeeeeeeeeeeee',
             'fffffffffffffffffff',
             'ggggggggggggggggggg',
             'hhhhhhhhhhhhhhhhhhh',
             'iiiiiiiiiiiiiiiiiii',
             'jjjjjjjjjjjjjjjjjjj']

SOME_NUMBER = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 11, 22, 33, 44, 55, 66, 77, 88, 99, 100,
               51, 52, 53, 54, 455, 56, 57, 58, 59, 50, 511, 522, 533, 544, 555, 566, 577, 588, 599, 5100]

def example_data(length):
    out = StringIO()
    writer = csv.DictWriter(out, example_data_row.keys())
    writer.writeheader()
    for i in range(length):
        example_data_row['text1'] = random.choice(SELECTION)
        example_data_row['classifier'] = random.choice(SELECTION)
        example_data_row['some_number'] = random.choice(SOME_NUMBER)
        writer.writerow(example_data_row)

    return out.getvalue()


def main():
    print "Building datasets"
    datasets = [example_data(l) for l in (1000, 5000, 10000, 20000, 50000, 100000, 200000, 400000)]

    latest_datasets = []
    while True:
        ds = random.choice(datasets)
        key = ''.join(random.choice(string.ascii_uppercase) for _ in range(6))
        t0 = time.time()
        response = requests.post("http://localhost:8888/qcache/dataset/{key}".format(key=key),
                                 headers={'Content-type': 'text/csv'}, data=ds)
        print "Posted {key}={size}, response={response}, duration={duration}".format(
            key=key, size=len(ds), response=response.status_code, duration=time.time()-t0)

        # Keep the last 40 inserted
        latest_datasets.append(key)
        latest_datasets = latest_datasets[-40:]

        for _ in range(random.randint(0, 5)):
            query = dict(select=[['distinct', 'text1', 'text2', 'a_status', 'some_number']],
                         where=['==', 'classifier', "'{}'".format(random.choice(SELECTION))],
                         limit=50)
            params = {'q': json.dumps(query)}

            ds_key = random.choice(latest_datasets)

            t0 = time.time()
            response = requests.get("http://localhost:8888/qcache/dataset/{key}".format(key=ds_key),
                                    params=params, headers={'Accept': 'application/json'})

            if response.status_code == 200:
                print "Success length: {length}, duration: {duration}".format(
                    status=response.status_code, length=len(json.loads(response.content)),
                    duration=time.time()-t0)
            else:
                print "Response status: {status}, content: {content}, duration: {duration}".format(
                    status=response.status_code, content=response.content, duration=time.time()-t0)

        time.sleep(0.5)

if __name__ == '__main__':
    main()