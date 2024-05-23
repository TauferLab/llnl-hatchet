hatchet_python_sources = [
    '__init__.py',
    'frame.py',
    'graph.py',
    'graphframe.py',
    'node.py',
    'version.py'
]

py.install_sources(
    hatchet_python_sources,
    pure: false,
    subdir: 'hatchet'
)

subdir('cython_modules')
subdir('external')
subdir('query')
subdir('readers')
subdir('util')
subdir('vis')
subdir('writers')