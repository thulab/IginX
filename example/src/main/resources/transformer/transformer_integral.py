import pandas as pd
import numpy as np

class IntegralTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        timestamp = df[df.keys()[0]]
        del df[df.keys()[0]]
        df = df.fillna(value=np.nan)
        df = df.abs()
        res = df.apply(lambda g: np.trapz(g, x=timestamp))
        return res.tolist()
