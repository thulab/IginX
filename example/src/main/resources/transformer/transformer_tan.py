import pandas as pd
import math
import numpy as np

class TanTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) else math.tan(x)))
        return ret.values.tolist()