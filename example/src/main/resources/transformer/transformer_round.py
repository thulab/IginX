import pandas as pd

class RoundTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.round())
        return ret.values.tolist()
