import pandas as pd

class AddOneTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows) + 1
        return df.values.tolist()
