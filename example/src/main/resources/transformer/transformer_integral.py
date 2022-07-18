import pandas as pd
import numpy as np

class IntegralTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        timestamp = df[df.keys()[0]].tolist()
        del df[df.keys()[0]]
        df = df.fillna(value=np.nan)
        df = df.abs()
        res = []
        for i in range(df.shape[1]):
            rawData = df[df.keys()[i]].tolist()
            s = 0
            count = 0
            currTs = -1
            currVal = 0
            for j in rawData:
                if not np.isnan(j):
                    if not currTs == -1:
                        s += (j + currVal) * (timestamp[count] - currTs) / 2   
                    currVal = j
                    currTs = timestamp[count]
                count += 1
            res.append(s)
        return res
