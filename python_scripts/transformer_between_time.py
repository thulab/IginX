"""
Select values between particular times of the day (e.g., 9:00-9:30 AM).
"""
from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df.between_time(start_time, end_time, include_start=NoDefault.no_default,
                               include_end=NoDefault.no_default, inclusive=None,
                               axis=None)  # (start_time, end_time, include_start=NoDefault.no_default,
        # include_end=NoDefault.no_default, inclusive=None, axis=None)
