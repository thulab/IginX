import pandas as pd
import numpy as np

class AddOneTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows) + 1
        df = df.replace({np.nan: None})
        return df.values.tolist()
