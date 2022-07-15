import pandas as pd
import numpy as np

class LastTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = []
        for col in df.columns:
            i = df[col].last_valid_index()
            if i == None:
                ret.append(np.NaN)
            else:
                ret.append(df[col][i])
        return ret.values.tolist()
