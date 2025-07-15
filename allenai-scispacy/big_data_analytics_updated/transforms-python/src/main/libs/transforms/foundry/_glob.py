# Copyright 2017 Palantir Technologies, Inc.
import os
import re


def glob2regex(pat: str) -> str:
    """Translate a shell PATTERN to a regular expression.

    There is no way to quote meta-characters.

    http://stackoverflow.com/questions/27726545/python-glob-but-against-a-list-of-strings-rather-than-the-filesystem
    """
    # pylint: disable=too-many-branches
    i, n = 0, len(pat)
    res = ""
    while i < n:
        c = pat[i]
        i = i + 1
        if c == "*":
            if i < n and pat[i] == "*" and i + 1 < n and pat[i + 1] == "/":
                res = res + "(.*)"
                if i + 2 < n and pat[i + 2] != "*":
                    res = res + r"\/"
                i = i + 2
            else:
                res = res + "([^\\" + os.path.sep + "]*)"
        elif c == "?":
            res = res + "(.)"
        elif c == "[":
            j = i
            if j < n and pat[j] == "!":
                j = j + 1
            if j < n and pat[j] == "]":
                j = j + 1
            while j < n and pat[j] != "]":
                j = j + 1
            if j >= n:
                res = res + "\\["
            else:
                stuff = pat[i:j].replace("\\", "\\\\")
                i = j + 1
                if stuff[0] == "!":
                    stuff = "^" + stuff[1:]
                elif stuff[0] == "^":
                    stuff = "\\" + stuff
                res = f"{res}([{stuff}])"
        else:
            res = res + re.escape(c)
    return r"(?ms)" + res + r"\Z"
