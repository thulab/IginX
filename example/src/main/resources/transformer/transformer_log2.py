import pandas as pd
import math
import numpy as np

class Log2Transformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(value=np.nan)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) or x <= 0 else math.log2(x)))
        return ret.values.tolist()
