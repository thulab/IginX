from transformer import BaseTransformer


def load_class(file_name, class_name):
    """
    load a sub class of BaseTransformer by file_name and class_name
    """
    try:
        import_module = __import__(file_name)
        import_class = getattr(import_module, class_name)

        clazz = import_class()
        if issubclass(import_class, BaseTransformer):
            return clazz
        else:
            raise RuntimeError("The loaded class is not a sub class of BaseTransformer.")
    except Exception as e:
        msg = str(e)
        print("Fail to load %s in file %s.py, because %s" % (class_name, file_name, msg))
    pass
