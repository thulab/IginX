import numpy as np
import pandas as pd

class DerivativeTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        time = df[0]
        data = df[1]
        length = len(time)
        res = []
        currTime = None
        currData = None
        for i in range(length):
            if data[i] != None and not np.isnan(data[i]):
                if currTime != None:
                    res.append([time[i],(data[i] - currData)/(time[i] - currTime)])
                currTime = time[i]
                currData = data[i]
        if currTime == None:
            res.append(np.NaN)
        return res
