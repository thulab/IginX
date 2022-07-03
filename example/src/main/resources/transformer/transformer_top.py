import pandas as pd


class TopTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n, index):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.nlargest(n, df.keys()[index]))
        return ret.values.tolist()
