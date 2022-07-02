import pandas as pd

class MovingAverageTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.rolling(window=n).mean()[n-1:])
        return ret.values.tolist()
