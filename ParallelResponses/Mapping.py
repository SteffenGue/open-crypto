from collections import deque
from dictionary import ExceptionDict
from utilities import TYPE_CONVERSION


class Mapping:
    """Class representing mapping data and logic.

    Class representing mapping data und further functionality provided
    with methods.

    Attributes:
        key:
            String being the keyword indicating one or several
            database table columns. See "database_column_mapping"
            in "config.yaml".
        path:
            An ordered list of keys used for traversal through the
            response dict with the intention of returning the value wanted
            for the database.
        types:
            An ordered sequence of types and
            additional parameters (if necessary). Is used to conduct
            type conversions within the method "extract_value()".
    """

    def __init__(self,
                 key: str,
                 path: list,
                 types: list):
        """Constructor of Mapping.

        Constructor method for constructing method objects.

        Args:
            key:
                String being the keyword indicating one or several
                database table columns. See "database_column_mapping"
                in "config.yaml".
            path:
                An ordered list of keys used for traversal through the
                response dict with the intention of returning the value wanted
                for the database.
            types:
                An ordered sequence of types and
                additional parameters (if necessary). Is used to conduct
                type conversions within the method "extract_value()".
        """

        self.key = key
        self.path = path
        self.types = types

    def convert_type(self, value, types_queue: deque):
        """Converts the value via type conversions.

        Helper method to convert the given value via a queue of
        type conversions .

        Args:
            value:
                The value to get converted to another type.
            types_queue:
                The queue of type conversion instructions.

        Returns:
            The converted value.
        """

        current_type = types_queue.popleft()
        next_type = None

        result = value

        while types_queue:
            next_type = types_queue.popleft()

            types_tuple = (current_type,
                           next_type)

            conversion = TYPE_CONVERSION[types_tuple]

            params = list()

            if conversion["params"]:
                # pylint: disable=unused-variable
                for number in range(0, conversion["params"]):
                    params.append(types_queue.popleft())


            if not result and isinstance(result, (str, list)):
                result = None
            else:
                result = conversion["function"](result,
                                                *params)

            current_type = next_type

        return result

    def traverse_path(self, response: dict, path_queue: deque) -> dict:
        """Traverses the path on a response.

        Helper method for traversing the path on the given
        response dict (subset).

        Args:
            response:
                The response dict (subset).
            path_queue:
                The queue of path traversal instructions.

        Returns:
            The traversed response dict.
        """

        path_element = path_queue.popleft()

        if path_element == "dict_key":
            # Special case to extract value from "dict_key"
            traversed = list(response.keys())
        elif path_element == "dict_values":
            # Special case to extract value from "dict_values"
            traversed = list(response.values())
        elif path_element == "list_key":
            # Special case with the currency_pair prior to a list
            traversed = list(response.keys())
        elif path_element == "list_values":
            traversed = list(response.values())
        elif path_element == []:
            # Special case to extract multiple values from a single list ["USD","BTC",...]
            traversed = response
        elif is_scalar(response):
            return None
        else:
            traversed = response[path_element]

        return traversed

    def extract_value(self,
                      response: dict,
                      path_queue: deque = None,
                      types_queue=None,
                      iterate=True):
        """Extracts the value specfied by "self.path".

        Extracts the value specified by the path sequence and converts it
        using the "types" specified.

        Args:
            response:
                The response dict (JSON) returned by an API request.
            path_queue:
                The queue of path traversal instructions.
            types_queue:
                The queue of type conversion instructions.
            iterate:
                Whether still an auto-iteration is possible.

        Returns:
            The value specified by "path_queue" and converted
            using "types_queue".
            Can be a list of values which get extracted iteratively from
            the response.
        """
        #print(types_queue)
        #print(self.key)
        try:
            if path_queue is None:
                path_queue = deque(self.path)

            if types_queue is None:
                types_queue = deque(self.types)

            if not response:
                return None

            if not path_queue:
                return self.convert_type(None, types_queue)

            while path_queue:

                if iterate and \
                        isinstance(response, list):
                    # Iterate through list of results
                    result = list()

                    if len(response) == 1:
                        response = response[0]
                    else:
                        for item in response:

                            if is_scalar(item):
                                return self.extract_value(response,
                                                          path_queue,
                                                          types_queue,
                                                          iterate=False)

                            result.append(
                                self.extract_value(
                                    item,
                                    deque(path_queue),
                                    deque(types_queue)
                                )
                            )


                        return result

                elif is_scalar(response):
                    # Return converted scalar value
                    return self.convert_type(response, types_queue)

                else:
                    # Traverse path
                    response = self.traverse_path(response, path_queue)

            if types_queue:

                if isinstance(response, list):

                    result = list()

                    for item in response:
                        result.append(
                            self.convert_type(item, deque(types_queue))
                        )


                    response = result

                else:
                    response = self.convert_type(response, types_queue)

            return response
        except Exception:
            exception = ExceptionDict()
            exception.get_dict()['{}'.format(exchange)] = 1
            pass

    def __str__(self) -> str:
        """String representation of a Mapping"""

        string_path = list()

        for item in self.path:
            string_path.append(str(item))

        return " / ".join(string_path) + " -> " + str(self.key)


def is_scalar(value) -> bool:
    """Whether a value is a scalar or not.

    Convenience function returning a bool whether the provided value is
    a single value or not.
    Strings count as True although they are iterable.

    Args:
        value:
            The value to evaluate concerning whether it is a single value
            or multiple values (iterable).

    Returns:
        Bool indicating whether the provided value is a single value or not.
    """

    if isinstance(value, str):
        return True

    try:
        iter(value)
        return False
    except TypeError:
        return True
