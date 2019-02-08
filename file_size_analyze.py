#!/usr/bin/env python3

import sys
import os


def sizeof_fmt(num, suffix='B'):
    for unit in ' KMGTPEZ':
        if abs(num) < 1000:
            return "%3.0f %s%s" % (num, unit, suffix)
        num /= 1000
    return "%.2f %s%s" % (num, 'Y', suffix)


thresholds = []
for i in range(1, 13):
    thresholds.append(10 ** i)

counts = [0 for _ in thresholds]
dircount = 0
for root, dirs, files in os.walk(sys.argv[1]):
    dircount += 1
    for file in files:
        try:
            size = os.stat(os.path.join(root, file)).st_size
        except:
            print('Error:', os.path.join(root, file))
            continue
        for i, t in enumerate(thresholds):
            if size < t:
                counts[i] += 1
                break

print('Dir count =', dircount)
print('File count =', sum(counts))
last = 0
for i, t in enumerate(thresholds):
    print(sizeof_fmt(last), '<= size <', sizeof_fmt(t), ':', counts[i])
    last = t
