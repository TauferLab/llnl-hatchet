# Setup the Meson Project
project('llnl-hatchet', 'cython',
    version: run_command(
        ['python3 hatchet/utils/print_version.py'],
        check: true
    ).stdout().strip()
)

# Get the Meson Python object, a dependency object to Python for extension modules,
# and the path to the top-level install directory
py = import('python').find_installation(pure: false)
py_dep = py.dependency()
py_top_install_dir = py.get_install_dir()

# Enter the 'hatchet' subdirectory
subdir('hatchet')