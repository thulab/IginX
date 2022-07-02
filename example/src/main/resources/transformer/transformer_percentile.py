import pandas as pd


class PercentileTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.quantile(n)).transpose()
        return ret.values.tolist()
