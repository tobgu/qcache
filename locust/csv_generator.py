import csv
import random
import string
import sys
from io import StringIO


def generate_csv(length):
    string_set = ['adffgfa', 'grfdsgfd', 'spivljfd', 'afdweoww', 'qpqodjap',
                  'edadggf', 'asdfhsrh', 'ypigdfgf', 'vnbrasdf', 'qasdfggg']
    fieldnames = ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff', 'ggg', 'hhh', 'iii', 'jjj', 'kkk', 'lll', 'mmm', 'nnn', 'ooo']

    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for _ in range(length):
        writer.writerow(
            {
                'aaa': 1001,
                'bbb': 1000 * random.random(),
                'ccc': 1000000 * random.random(),
                'ddd': 100000000 * random.random(),
                'eee': 100000000 * random.random(),
                'fff': 100000000 * random.random(),
                'ggg': random.randint(0, 100000),
                'hhh': random.randint(0, 10000000),
                'iii': random.choice(string_set),
                'jjj': random.choice(string_set),
                'kkk': random.choice(string_set),
                'lll': ''.join(random.choice(string.ascii_uppercase) for _ in range(3)),
                'mmm': ''.join(random.choice(string.ascii_uppercase) for _ in range(6)),
                'nnn': ''.join(random.choice(string.ascii_uppercase) for _ in range(9)),
                'ooo': ''.join(random.choice(string.ascii_uppercase) for _ in range(12)),
            })

    print(out.getvalue())


if __name__ == "__main__":
    generate_csv(int(sys.argv[1]))
