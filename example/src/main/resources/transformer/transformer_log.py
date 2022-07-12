import pandas as pd
import math
import numpy as np

class LogTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) or x <= 0 else math.log(x, n)))
        return ret.values.tolist()
