import numpy as np

class ElapsedTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        res = []
        arr = np.array(rows)
        timestamp = arr[:,0]
        row = arr[:,1]
        l = len(timestamp)
        pos = 0
        currts = -1
        while pos < l:
            while pos < l and (row[pos] == None or np.isnan(row[pos])):
                pos += 1
            if pos < l:
                newts = timestamp[pos]
                if currts != -1:
                    res.append(newts - currts)
                currts = newts
                pos += 1
        return res
