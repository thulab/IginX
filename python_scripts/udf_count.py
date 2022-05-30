class UDFCount:
    def __init__(self):
        pass

    def transform(self, rows, params):
        res = []
        for row in zip(*rows):
            count = 0
            for num in row:
                if num is not None:
                    count += 1
            res.append(count)
        return res
