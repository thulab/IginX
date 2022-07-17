import pandas as pd
import math
import numpy as np

class FloorTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        timestamp = df[df.keys()[0]]
        del df[df.keys()[0]]
        df = df.fillna(value=np.nan)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.nan if np.isnan(x) else math.floor(x)))
        ret.insert(0, 'time', timestamp)
        return ret.values.tolist()
