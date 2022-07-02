import pandas as pd


class SpreadTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.max(axis=0)-df.min(axis=0)).transpose()
        return ret.values.tolist()
