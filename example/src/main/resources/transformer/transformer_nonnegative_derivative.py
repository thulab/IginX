import pandas as pd
import math

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
            if data[i] != None:
                if currTime != None:
                    res.append(math.abs((data[i] - currData)/(time[i] - currTime)))
                currTime = time[i]
                currData = data[i]
        if currTime == None:
            res.append(np.NaN)
        return res