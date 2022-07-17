import pandas as pd
import math
import numpy as np

class AsinTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        timestamp = df[df.keys()[0]]
        del df[df.keys()[0]]
        df = df.fillna(value=np.nan)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) or x > 1 or x < -1 else math.asin(x)))
        ret.insert(0, 'time', timestamp)
        return ret.values.tolist()
