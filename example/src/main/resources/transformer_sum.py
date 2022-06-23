import pandas as pd


class SumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.sum(axis=0)).transpose()
        return ret.values.tolist()
