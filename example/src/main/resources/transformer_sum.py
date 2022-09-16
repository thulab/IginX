import pandas as pd


class SumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows[1:], columns=rows[0])
        df = pd.DataFrame(data=df.sum(axis=0)).transpose()
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
