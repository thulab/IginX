import pandas as pd
import math

class FloorTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.floor(x)))
        return ret.values.tolist()
