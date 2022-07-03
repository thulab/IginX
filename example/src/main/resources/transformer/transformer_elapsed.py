import pandas as pd


class ElapsedTransformer:
    def __init__(self):
        pass

    def transform(self, timestamp, row):
        res = []
        l = len(timestamp)
        pos = 0
        currts = -1
        while pos < l:
            while pos < l and row[pos] == None:
                pos += 1
            if pos < l:
                newts = timestamp[pos]
                if currts != -1:
                    res.append(newts - currts)
                currts = newts
                pos += 1
        return res
