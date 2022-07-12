import pandas as pd
import math
import numpy as np

class Log10Transformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: np.NaN if np.isnan(x) or x <= 0 else math.log10(x)))
        return ret.values.tolist()
