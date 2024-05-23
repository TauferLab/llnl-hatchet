cython_module_names = [
    'graphframe_modules',
    'reader_modules'
]

foreach mod_name : cython_module_names
    py.extension_module(
        mod_name,
        mod_name + '.pyx',
        dependencies: py_dep,
        subdir: 'hatchet/cython_modules/lib',
        install: true
    )
endforeach