import pandas as pd
import numpy as np

class AbsTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.fillna(value=np.nan)
        return df.abs().values.tolist()
