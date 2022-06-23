from transformer import BaseTransformer


def load_class(file_name, class_name):
    """
    load a sub class of BaseTransformer by file_name and class_name

    return:
    clazz: loaded class
    succ: is class successfully loaded by name
    """
    try:
        import_module = __import__(file_name)
        import_class = getattr(import_module, class_name)

        clazz = import_class()
        return clazz, True
        # if issubclass(import_class, BaseTransformer):
        #     return clazz, True
        # else:
        #     print("The loaded class is not a sub class of BaseTransformer.")
        #     return None, False
    except Exception as e:
        msg = str(e)
        print("Fail to load %s in file %s.py, because %s" % (class_name, file_name, msg))
        return None, False
    pass
