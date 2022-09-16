"""
Return the bool of a single element Series or DataFrame.
This must be a boolean scalar value, either True or False.
It will raise a ValueError if the Series or DataFrame does not have exactly 1 element, or that element is not boolean (integer values 0 and 1 will also raise an exception).
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.bool()
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
