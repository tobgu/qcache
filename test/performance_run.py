import requests
from time import time

if __name__ == '__main__':
    i = 0
    results = []
    t0 = time()
    while i <= 1000:
        requests.get('http://localhost:8088/status')
        t1 = time()
        results.append(t1 - t0)
        t0 = t1
        i += 1

    results.sort()
    print("Median: %s, 90perc: %s, 99perc: %s" % (results[500], results[900], results[990]))
