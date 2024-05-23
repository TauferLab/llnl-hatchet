hatchet_vis_python_sources = [
    '__init__.py',
    'loader.py',
    'static_fixer.py'
]

py.install_sources(
    hatchet_vis_python_sources,
    pure: false,
    subdir: 'hatchet' / 'vis'
)

hatchet_vis_install_dir = py_top_install_dir / 'hatchet' / 'vis'

install_subdir(
    'external',
    install_dir: hatchet_vis_install_dir
)
install_subdir(
    'scripts',
    install_dir: hatchet_vis_install_dir
)
install_subdir(
    'static',
    install_dir: hatchet_vis_install_dir
)
install_subdir(
    'styles',
    install_dir: hatchet_vis_install_dir
)
install_subdir(
    'templates',
    install_dir: hatchet_vis_install_dir
)

install_data(
    'package.json', 'README.md', 'requirements.txt', 'webpack.config.js',
    install_dir: hatchet_vis_install_dir
)