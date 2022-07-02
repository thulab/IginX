import pandas as pd


class AbsTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        return df.abs().values.tolist()
