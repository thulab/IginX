"""
Return unbiased skew over requested axis.
Normalized by N-1.
"""
from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        # finding skewness along the index axis
        return df.skew(axis=0, skipna=True)  # (axis=NoDefault.no_default, skipna=True, level=None,
        # numeric_only=None, **kwargs)
