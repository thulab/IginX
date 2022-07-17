import pandas as pd

class CumulativeSumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        timestamp = df[df.keys()[0]]
        del df[df.keys()[0]]
        df = df.fillna(0)
        ret = pd.DataFrame(data=df.cumsum())
        ret.insert(0, 'time', timestamp)
        return ret.values.tolist()
