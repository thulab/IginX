import math


class UDFSin:
    def __init__(self):
        pass

    def transform(self, row):
        res = []
        for num in row:
            res.append(math.sin(num))
        return res
