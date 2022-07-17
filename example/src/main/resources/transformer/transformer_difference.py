import numpy as np

class DifferenceTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        arr = np.array(rows)
        timestamp = arr[:,0]
        data = arr[:,1]   
        length = len(data)
        res = []
        currData = None
        for i in range(length):
            if data[i] != None and not np.isnan(data[i]):
                if currData != None:
                    res.append([timestamp[i], data[i] - currData])
                currData = data[i]
        if len(res) == 0:
            res.append(np.NaN)
        return res
