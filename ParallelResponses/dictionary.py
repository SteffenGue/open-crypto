class Singleton(type):
    """
    This metaclass provides the required properties to create a class similar to the Singleton pattern.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ExceptionDict(metaclass=Singleton):
    """
    This class represents the dictionary to 'persist' the exceptions. Any instance of the class will contain the same
    object ( dictionary ). To achieve this pattern, the class contains the metaclass Singleton.
    An Example for the dictionary could be:
    x = {'bitrue': 1,
         'idax': 1}
    If a given exchange exists in the dictionary, its exception_counter will be increased by one. This is handled by
    the classmethod update_exceptions from the class Exchange, in which the flag for the activity is persisted.
    """
    pass

    def __init__(self):
        """
        The Initiation of the the class with the variable x for the dictionary.
        """
        self.__x = {}

    def get_dict(self):
        """
        Getter method for the variable of the dictionary.
        return: the dictionary
        """
        return self.__x