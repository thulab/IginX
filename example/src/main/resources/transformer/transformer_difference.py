import pandas as pd

class DifferenceTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.diff()[1:])
        return ret.values.tolist()
