# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#




from collections import Mapping

import os

from pyLibrary import dot
from pyLibrary.dot import set_default, wrap, unwrap
from pyLibrary.parsers import URL


DEBUG = False
_convert = None
_Log = None
_Except = None


def _late_import():
    global _convert
    global _Log
    global _Except
    from pyLibrary import convert as _convert
    from pyLibrary.debugs.logs import Log as _Log
    from pyLibrary.debugs.exceptions import Except as _Except

    _ = _convert
    _ = _Log
    _ = _Except


def get(url):
    """
    USE json.net CONVENTIONS TO LINK TO INLINE OTHER JSON
    """
    if not _Log:
        _late_import()

    if url.find("://") == -1:
        _Log.error("{{url}} must have a prototcol (eg http://) declared", url=url)

    base = URL("")
    if url.startswith("file://") and url[7] != "/":
        if os.sep=="\\":
            base = URL("file:///" + os.getcwd().replace(os.sep, "/").rstrip("/") + "/.")
        else:
            base = URL("file://" + os.getcwd().rstrip("/") + "/.")
    elif url[url.find("://") + 3] != "/":
        _Log.error("{{url}} must be absolute", url=url)

    phase1 = _replace_ref(wrap({"$ref": url}), base)  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    try:
        phase2 = _replace_locals(phase1, [phase1])
        return wrap(phase2)
    except Exception as e:
        _Log.error("problem replacing locals in\n{{phase1}}", phase1=phase1)


def expand(doc, doc_url):
    """
    ASSUMING YOU ALREADY PULED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE
    """
    if not _Log:
        _late_import()

    if doc_url.find("://") == -1:
        _Log.error("{{url}} must have a prototcol (eg http://) declared", url=doc_url)

    phase1 = _replace_ref(doc, URL(doc_url))  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals(phase1, [phase1])
    return wrap(phase2)


def _replace_ref(node, url):
    if url.path.endswith("/"):
        url.path = url.path[:-1]

    if isinstance(node, Mapping):
        ref = None
        output = {}
        for k, v in list(node.items()):
            if k == "$ref":
                ref = URL(v)
            else:
                output[k] = _replace_ref(v, url)

        if not ref:
            return output

        node = output

        if not ref.scheme and not ref.path:
            # DO NOT TOUCH LOCAL REF YET
            output["$ref"] = ref
            return output

        if not ref.scheme:
            # SCHEME RELATIVE IMPLIES SAME PROTOCOL AS LAST TIME, WHICH
            # REQUIRES THE CURRENT DOCUMENT'S SCHEME
            ref.scheme = url.scheme

        # FIND THE SCHEME AND LOAD IT
        if ref.scheme in scheme_loaders:
            new_value = scheme_loaders[ref.scheme](ref, url)
        else:
            raise _Log.error("unknown protocol {{scheme}}", scheme=ref.scheme)

        if ref.fragment:
            new_value = dot.get_attr(new_value, ref.fragment)

        if DEBUG:
            _Log.note("Replace {{ref}} with {{new_value}}", ref=ref, new_value=new_value)

        if not output:
            output = new_value
        else:
            output = unwrap(set_default(output, new_value))

        if DEBUG:
            _Log.note("Return {{output}}", output=output)

        return output
    elif isinstance(node, list):
        output = [_replace_ref(n, url) for n in node]
        # if all(p[0] is p[1] for p in zip(output, node)):
        #     return node
        return output

    return node


def _replace_locals(node, doc_path):
    if isinstance(node, Mapping):
        # RECURS, DEEP COPY
        ref = None
        output = {}
        for k, v in list(node.items()):
            if k == "$ref":
                ref = v
            else:
                output[k] = _replace_locals(v, [v] + doc_path)

        if not ref:
            return output

        # REFER TO SELF
        frag = ref.fragment
        if frag[0] == ".":
            # RELATIVE
            for i, p in enumerate(frag):
                if p != ".":
                    if i>len(doc_path):
                        _Log.error("{{frag|quote}} reaches up past the root document",  frag=frag)
                    new_value = dot.get_attr(doc_path[i-1], frag[i::])
                    break
            else:
                new_value = doc_path[len(frag) - 1]
        else:
            # ABSOLUTE
            new_value = dot.get_attr(doc_path[-1], frag)

        new_value = _replace_locals(new_value, [new_value] + doc_path)

        if not output:
            return new_value  # OPTIMIZATION FOR CASE WHEN node IS {}
        else:
            return unwrap(set_default(output, new_value))

    elif isinstance(node, list):
        candidate = [_replace_locals(n, [n] + doc_path) for n in node]
        # if all(p[0] is p[1] for p in zip(candidate, node)):
        #     return node
        return candidate

    return node


###############################################################################
## SCHEME LOADERS ARE BELOW THIS LINE
###############################################################################

def get_file(ref, url):
    from pyLibrary.env.files import File

    if ref.path.startswith("~"):
        home_path = os.path.expanduser("~")
        if os.sep == "\\":
            home_path = "/" + home_path.replace(os.sep, "/")
        if home_path.endswith("/"):
            home_path = home_path[:-1]

        ref.path = home_path + ref.path[1::]
    elif not ref.path.startswith("/"):
        # CONVERT RELATIVE TO ABSOLUTE
        if ref.path[0] == ".":
            num_dot = 1
            while ref.path[num_dot] == ".":
                num_dot += 1

            parent = url.path.rstrip("/").split("/")[:-num_dot]
            ref.path = "/".join(parent) + ref.path[num_dot:]
        else:
            parent = url.path.rstrip("/").split("/")[:-1]
            ref.path = "/".join(parent) + "/" + ref.path


    path = ref.path if os.sep != "\\" else ref.path[1::].replace("/", "\\")

    try:
        if DEBUG:
            _Log.note("reading file {{path}}", path=path)
        content = File(path).read()
    except Exception as e:
        content = None
        _Log.error("Could not read file {{filename}}", filename=path, cause=e)

    try:
        new_value = _convert.json2value(content, params=ref.query, flexible=True, leaves=True)
    except Exception as e:
        if not _Except:
            _late_import()

        e = _Except.wrap(e)
        try:
            new_value = _convert.ini2value(content)
        except Exception as f:
            raise _Log.error("Can not read {{file}}", file=path, cause=e)
    new_value = _replace_ref(new_value, ref)
    return new_value


def get_http(ref, url):
    from pyLibrary.env import http

    params = url.query
    new_value = _convert.json2value(http.get(ref), params=params, flexible=True, leaves=True)
    return new_value


def get_env(ref, url):
    # GET ENVIRONMENT VARIABLES
    ref = ref.host
    try:
        new_value = _convert.json2value(os.environ[ref])
    except Exception as e:
        new_value = os.environ[ref]
    return new_value


def get_param(ref, url):
    # GET PARAMETERS FROM url
    param = url.query
    new_value = param[ref.host]
    return new_value


scheme_loaders = {
    "http": get_http,
    "file": get_file,
    "env": get_env,
    "param": get_param
}
