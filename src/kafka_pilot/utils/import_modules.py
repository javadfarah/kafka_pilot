import importlib
import pathlib
import pkgutil
import sys


def import_project_modules(path_or_package: str):
    """Import all modules in the project recursively by package name or directory path."""

    # If it's a directory, convert to package
    path = pathlib.Path(path_or_package)
    if path.exists() and path.is_dir():
        # Add parent dir to sys.path so package can be imported
        sys.path.insert(0, str(path.parent))
        root_package = path.name
    else:
        root_package = path_or_package

    package = importlib.import_module(root_package)
    pkg_file = getattr(package, "__file__", None)
    if pkg_file is not None:
        package_paths = [str(pathlib.Path(pkg_file).parent)]
    elif getattr(package, "__path__", None):
        package_paths = list(package.__path__)
    else:
        return

    for module_info in pkgutil.walk_packages(package_paths, prefix=root_package + "."):
        importlib.import_module(module_info.name)
