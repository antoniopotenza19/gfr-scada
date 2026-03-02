"""
Facade module: re-export models from the package implementation.

This file intentionally re-exports model classes from `app.models.models`.
It exists to keep `import app.models` working for code that expects a single module.
"""
from importlib import import_module

_mod = import_module('app.models.models')
globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith('_')})


