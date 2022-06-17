class UDFCount:
    def __init__(self):
        pass

    def transform(self, rows):
        res = []
        for num in rows[0]:
            res.append(len(rows))
        return res
