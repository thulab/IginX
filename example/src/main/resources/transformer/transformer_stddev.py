import pandas as pd
import numpy as np

class StddevTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(value=np.nan)
        ret = pd.DataFrame(data=df.std(axis=0)).transpose()
        return ret.values.tolist()
