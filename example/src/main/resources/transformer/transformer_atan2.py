import pandas as pd
import math

class Atan2Transformer:
    def __init__(self):
        pass

    def transform(self, rowy, rowx):
        dfy = pd.DataFrame(rowy)
        dfx = pd.DataFrame(rowx)
        ret = pd.DataFrame(data=(dfy / dfx).applymap(lambda x: math.atan(x))).transpose()
        return ret.values.tolist()[0]