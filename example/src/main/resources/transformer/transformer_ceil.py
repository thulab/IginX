import pandas as pd
import math
import numpy as np

class CeilTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: None if np.isnan(x) else math.ceil(x)))
        return ret.values.tolist()
