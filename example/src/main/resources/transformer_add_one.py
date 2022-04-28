from transformer import BaseTransformer


class AddOneTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df + 1
        pass
