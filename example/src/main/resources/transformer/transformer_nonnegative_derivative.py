import numpy as np

class NonNegativeDerivativeTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        arr = np.array(rows)
        time = arr[:,0]
        data = arr[:,1]
        length = len(time)
        res = []
        currTime = None
        currData = None
        for i in range(length):
            if data[i] != None and not np.isnan(data[i]):
                if currTime != None:
                    res.append([time[i],abs((data[i] - currData)/(time[i] - currTime))])
                currTime = time[i]
                currData = data[i]
        if currTime == None:
            res.append(np.NaN)
        return res
