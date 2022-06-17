class UDFMin:
    def __init__(self):
        pass

    def transform(self, rows):
        res = []
        for row in zip(*rows):
            min = None
            for num in row:
                if num is not None:
                    if min is None:
                        min = num
                    elif min > num:
                        min = num
            res.append(min)
        return res
