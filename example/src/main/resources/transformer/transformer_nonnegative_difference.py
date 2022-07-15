import math

class NonNegativeDifferenceTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        arr = np.array(rows)
        data = arr[:,0]
        length = len(data)
        res = []
        currData = None
        for i in range(length):
            if data[i] != None:
                if currData != None:
                    res.append(math.abs(data[i] - currData))
                currData = data[i]
        if currData == None:
            res.append(np.NaN)
        return res
