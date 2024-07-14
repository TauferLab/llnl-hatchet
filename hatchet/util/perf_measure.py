import os

try:
    from pycaliper import annotate_function
    from pycaliper.instrumentation import region_begin, region_end

    _PYCALIPER_AVAILABLE = True
except Exception:
    _PYCALIPER_AVAILABLE = False


def _validate_perf_plugin(plugin_name):
    if plugin_name.lower() == "caliper" and _PYCALIPER_AVAILABLE:
        return "caliper"
    return "none"


if "HATCHET_PERF_PLUGIN" in os.environ:
    _HATCHET_PERF_PLUGIN = _validate_perf_plugin(os.environ["HATCHET_PERF_PLUGIN"])
    _HATCHET_PERF_ENABLED = _HATCHET_PERF_PLUGIN != "none"
else:
    _HATCHET_PERF_ENABLED = False


def annotate(name=None):
    def inner_decorator(func):
        if not _HATCHET_PERF_ENABLED:
            return func
        else:
            real_name = name
            if name is None or name == "":
                real_name = func.__name__
            if _HATCHET_PERF_PLUGIN == "caliper":
                return annotate_function(name=real_name)(func)
            else:
                return func


def begin_code_region(name):
    if _HATCHET_PERF_ENABLED:
        if _HATCHET_PERF_PLUGIN == "caliper":
            region_begin(name)
            return


def end_code_region(name):
    if _HATCHET_PERF_ENABLED:
        if _HATCHET_PERF_PLUGIN == "caliper":
            region_end(name)
            return
