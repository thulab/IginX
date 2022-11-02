import pandas as pd


class AddOneTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows[1:], columns=rows[0]) + 1
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
