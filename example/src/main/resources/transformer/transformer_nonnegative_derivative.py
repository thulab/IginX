import pandas as pd

class NonNegativeDerivativeTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df2 = df.diff()[1:]
        df2 = df2.div(df2[0], axis=0)
        del df2[(df2.keys()[0])]
        ret = df2.abs()
        return ret.values.tolist()