import numpy as np
import pandas as pd

class NonNegativeDerivativeTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        arr = pd.DataFrame(rows)
        data = arr[1].tolist()
        time = arr[0].tolist()
        length = len(time)
        res = []
        currTime = None
        currData = None
        i = 0
        while i < length:
            if data[i] != None:
                if currTime != None:
                    res.append([time[i],abs((data[i] - currData)/(time[i] - currTime))])
                currTime = time[i]
                currData = data[i]
            i += 1
        if len(res) == 0:
            res.append(0)
        return res
