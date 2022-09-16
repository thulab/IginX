"""
Return unbiased skew over requested axis.
Normalized by N-1.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        # finding skewness along the index axis
        df = df.skew(axis=0, skipna=True)  # (axis=NoDefault.no_default, skipna=True, level=None,
        # numeric_only=None, **kwargs)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
