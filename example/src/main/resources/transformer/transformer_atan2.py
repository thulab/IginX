import pandas as pd
import math
import numpy as np

class Atan2Transformer:
    def __init__(self):
        pass

    def transform(self, rowx):
        arr = np.array(rows)
        rowy = arr[:,0]
        rowx = arr[:,1]
        dfy = pd.DataFrame(rowy)
        dfy = dfy.fillna(value=np.nan)
        dfx = pd.DataFrame(rowx)
        dfx = dfx.fillna(value=np.nan)
        ret = pd.DataFrame(data=(dfy / dfx).applymap(lambda x: np.nan if np.isnan(x) else math.atan(x))).transpose()
        return ret.values.tolist()[0]
