import pandas as pd
import numpy as np

class FirstTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(value=np.nan)
        del df[df.keys()[0]]
        ret = []
        for col in df.columns:
            i = df[col].first_valid_index()
            if i == None:
                ret.append(np.NaN)
            else:
                ret.append((df[col].tolist())[i])
        return ret
