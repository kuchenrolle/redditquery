#!/usr/bin/python3
import os
import warnings
from math import sqrt

# omit line information in warning
warnings.formatwarning = lambda message, category, *args, **kwargs: "{}: {}\n".format(category.__name__, str(message))


class Numberer:
    """Assigns ascending number to each individual input.
    Parameters
    ----------
    start : int, optional
            number to start from
    """

    def __init__(self, start = 0):
        self.known = dict()
        self.num_keys = start

    def get(self, key):
        """get/set number for term
        Parameters
        ----------
        key :   str
                key to get number for"""
        try:
            return self.known[key]
        except KeyError:
            self.num_keys += 1
            self.known[key] = self.num_keys
            return self.num_keys

    def remove_values(self, values):
        """Remove known terms by value
        Parameters
        ----------
        values :    iterable of int
                    values to be removed"""
        values = set(values)
        for key in list(self.known.keys()):
            if self.known[key] in values:
                del self.known[key]

def recursive_walk(directory):
    """Generator for traversing nested directory.
    Parameters
    ----------
    directory : string or path object
                Directory to be traversed
    """
    for folderName, subFolders, fileNames in os.walk(directory):
        if subFolders:
            for subFolder in subFolders:
                recursive_walk(os.path.join(folderName, subFolder))
        for fileName in fileNames:
            yield os.path.join(folderName, fileName)

def check_directory(directory):
    """Create directory if it's not a file. Issues warning if not empty.
    Parameters
    ----------
    directory : string or path object
                directory to be created
    """
    try:
        os.makedirs(directory)
    except OSError as e:
        if not os.path.isdir(directory):
            raise e
        elif os.listdir(directory):
            warnings.warn("Directory ({}) already contains files.".format(directory))
    return directory

def l2_norm(values):
    """Calculate l2 norm.
    Parameters
    ----------
    values : iterable object of int
             vector to calculate norm over
    """
    l2_norm = sqrt(sum([value**2 for value in values]))
    return l2_norm
