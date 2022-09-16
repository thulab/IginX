"""
Select values between particular times of the day (e.g., 9:00-9:30 AM).
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.between_time(start_time, end_time, include_start=NoDefault.no_default,
                               include_end=NoDefault.no_default, inclusive=None,
                               axis=None).values.tolist()  # (start_time, end_time, include_start=NoDefault.no_default,
        # include_end=NoDefault.no_default, inclusive=None, axis=None)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
