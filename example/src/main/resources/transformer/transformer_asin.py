import pandas as pd
import math
import numpy as np

class AsinTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) or x > 1 or x < -1 else math.asin(x)))
        return ret.values.tolist()