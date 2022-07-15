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
        dfx = pd.DataFrame(rowx)
        ret = pd.DataFrame(data=(dfy / dfx).applymap(lambda x: None if np.isnan(x) else math.atan(x))).transpose()
        return ret.values.tolist()[0]