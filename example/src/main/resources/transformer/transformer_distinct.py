import pandas as pd

class DistinctTransformer:
    def __init__(self):
        pass

    def transform(self, row):
        se = pd.Series(row)
        ret = se.unique()
        return ret.tolist()
