"""
Apply a function along an axis of the DataFrame.

Objects passed to the function are Series objects whose index is either the DataFrame’s index (axis=0) or the
DataFrame’s columns (axis=1). By default, (result_type=None), the final return type is inferred from the return type
of the applied function. Otherwise, it depends on the result_type argument.
"""

from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df.apply(func, axis=0, raw=False, result_type=None, args=(), **kwargs)
