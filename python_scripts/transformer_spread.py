import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.idxmax() - df.idxmin()
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
