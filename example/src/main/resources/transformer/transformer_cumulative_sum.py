import pandas as pd

class CumulativeSumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.cumsum())
        return ret.values.tolist()
