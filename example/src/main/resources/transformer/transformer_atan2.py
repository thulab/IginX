import pandas as pd
import math
import numpy as np

class Atan2Transformer:
    def __init__(self):
        pass

    def transform(self, rows):
        arr = np.array(rows)
        ts = arr[:,0].tolist()
        rowy = arr[:,1].tolist()
        rowx = arr[:,2].tolist()
        timestamp = pd.DataFrame(ts)
        dfy = pd.DataFrame(rowy)
        dfy = dfy.fillna(value=np.nan)
        dfx = pd.DataFrame(rowx)
        dfx = dfx.fillna(value=np.nan)
        ret = pd.DataFrame(data=(dfy / dfx).applymap(lambda x: np.nan if np.isnan(x) else math.atan(x)))
        ret.insert(0, 'time', timestamp)
        return ret.values.tolist()
