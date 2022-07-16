import pandas as pd
import numpy as np

class CumulativeSumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(0)
        ret = pd.DataFrame(data=df.cumsum())
        return ret.values.tolist()
