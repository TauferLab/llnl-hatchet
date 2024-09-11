# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import os

try:
    from pycaliper import annotate_function
    from pycaliper.instrumentation import begin_region, end_region

    _PYCALIPER_AVAILABLE = True
except Exception:
    _PYCALIPER_AVAILABLE = False

try:
    import perfflowaspect.aspect

    _PERFFLOWASPECT_AVAILABLE = True
except Exception:
    _PERFFLOWASPECT_AVAILABLE = False


def _validate_perf_plugin(plugin_name):
    print("Perf Plugin =", plugin_name)
    print("Caliper is available?", _PYCALIPER_AVAILABLE)
    if plugin_name.lower() == "caliper" and _PYCALIPER_AVAILABLE:
        return "caliper"
    elif plugin_name.lower() == "perfflowaspect" and _PYCALIPER_AVAILABLE:
        return "perfflowaspect"
    return "none"


if "HATCHET_PERF_PLUGIN" in os.environ:
    _HATCHET_PERF_PLUGIN = _validate_perf_plugin(os.environ["HATCHET_PERF_PLUGIN"])
    _HATCHET_PERF_ENABLED = _HATCHET_PERF_PLUGIN != "none"
else:
    _HATCHET_PERF_ENABLED = False


def annotate(name=None, fmt=None):
    def inner_decorator(func):
        if not _HATCHET_PERF_ENABLED:
            return func
        else:
            real_name = name
            if name is None or name == "":
                real_name = func.__name__
            if fmt is not None and fmt != "":
                real_name = fmt.format(real_name)
            if _HATCHET_PERF_PLUGIN == "caliper":
                return annotate_function(name=real_name)(func)
            elif _HATCHET_PERF_PLUGIN == "perfflowaspect":
                return perfflowaspect.aspect.critical_path(pointcut="around")(func)
            else:
                return func

    return inner_decorator


def begin_code_region(name):
    if _HATCHET_PERF_ENABLED:
        if _HATCHET_PERF_PLUGIN == "caliper":
            begin_region(name)
            return


def end_code_region(name):
    if _HATCHET_PERF_ENABLED:
        if _HATCHET_PERF_PLUGIN == "caliper":
            end_region(name)
            return
