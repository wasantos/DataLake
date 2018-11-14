# Utility module
import os

def pick(d: dict, *keys, default=None):
    """
    Extract subset of dict
    """
    return { key: d.get(key, default) for key in keys }

def augment(d: dict, **values):
    """
    Create copy of dict and augment it with new keys/values
    """
    return d.copy().update(values)

class EnvObject:
    """
    Wraps os.environ as a class
    """

    def __getattribute__(self, name):
        value = os.environ.get(name) or object.__getattribute__(self, name)

        return value
