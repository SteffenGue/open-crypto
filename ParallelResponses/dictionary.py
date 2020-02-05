class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ExceptionDict(metaclass=Singleton):
    pass
    def __init__(self):
        self.__x = {}
    def get_dict(self):
        return self.__x