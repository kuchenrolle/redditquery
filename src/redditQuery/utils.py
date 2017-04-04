#!/usr/bin/python3
import os
import warnings
from math import sqrt

# omit line information in warning
warnings.formatwarning = lambda message, category, *args, **kwargs: "{}: {}\n".format(category.__name__, str(message))


# assigns ascending number to each individual input
class Numberer:

    def __init__(self, start = 0):
        self.known = dict()
        self.num_keys = start

    # get/set number for term
    def get(self, key):
        try:
            return self.known[key]
        except KeyError:
            self.num_keys += 1
            self.known[key] = self.num_keys
            return self.num_keys

    # remove known terms by value
    def remove_values(self, values):
        for key in list(self.known.keys()):
            if self.known[key] in values:
                del self.known[key]

# generator for traversing nested directory
# returns paths to all files contained
def recursive_walk(directory):
    for folderName, subFolders, fileNames in os.walk(directory):
        if subFolders:
            for subFolder in subFolders:
                recursive_walk(os.path.join(folderName, subFolder))
        for fileName in fileNames:
            yield os.path.join(folderName, fileName)

# create directory, unless it exists and isn't a file
# issue a warning, in case directory contains files
def check_directory(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if not os.path.isdir(directory):
            raise e
        elif os.listdir(directory):
            warnings.warn("Directory ({}) already contains files.".format(directory))
    return directory

# calculate l2 norm for numeric vector
def l2_norm(values):
    l2_norm = sqrt(sum([value**2 for value in values]))
    return l2_norm
