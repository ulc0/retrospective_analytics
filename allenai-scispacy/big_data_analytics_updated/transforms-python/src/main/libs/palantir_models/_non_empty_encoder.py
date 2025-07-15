from conjure_python_client import ConjureEncoder


class NonEmptyConjureEncoder(ConjureEncoder):
    """
    Conjure encoder that removes all key value pairs in the object where the value is None.
    """

    def default(self, obj):
        output = ConjureEncoder.default(self, obj)
        if isinstance(output, dict):
            return {k: v for k, v in output.items() if v is not None}
        return output
