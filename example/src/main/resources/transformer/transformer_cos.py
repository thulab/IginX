import pandas as pd
import math

class CosTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.cos(x)))
        return ret.values.tolist()