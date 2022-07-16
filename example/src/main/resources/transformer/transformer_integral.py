import pandas as pd
import numpy as np

class IntegralTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(value=np.nan)
        df2 = df.diff()[1:]
        interval_area = df2.mul(df2[0]/2 , axis=0).abs()
        sum_area = interval_area.sum()[1:]
        return sum_area.tolist()
