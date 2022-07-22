import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        return (df.idxmax()-df.idxmin()).values.tolist()
