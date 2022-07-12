import pandas as pd
import math
import numpy as np

class PowTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) else math.pow(x, n)))
        return ret.values.tolist()
