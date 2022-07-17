import pandas as pd
import numpy as np

class AbsTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        timestamp = df[df.keys()[0]]
        del df[df.keys()[0]]
        df = df.fillna(value=np.nan)
        ret = df.abs()
        ret.insert(0, 'time', timestamp)
        return ret.values.tolist()
