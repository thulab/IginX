import pandas as pd
import math
import numpy as np

class AtanTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(value=np.nan)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.nan if np.isnan(x) else math.atan(x)))
        return ret.values.tolist()
