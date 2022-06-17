class UDFMax:
    def __init__(self):
        pass

    def transform(self, rows):
        res = []
        for row in zip(*rows):
            max = None
            for num in row:
                if num is not None:
                    if max is None:
                        max = num
                    elif max < num:
                        max = num
            res.append(max)
        return res
