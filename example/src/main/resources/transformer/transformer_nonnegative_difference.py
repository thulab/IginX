import pandas as pd

class NonNegativeDifferenceTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.diff().abs()[1:])
        return ret.values.tolist()
