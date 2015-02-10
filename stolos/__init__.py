import logging as _logging
log = _logging.getLogger('stolos')

import os.path as _p
import pkg_resources as _pkg_resources
__version__ = _pkg_resources.get_distribution(
    _p.basename(_p.dirname(_p.abspath(__file__)))).version


class Uninitialized(Exception):
    msg = (
        "Before you use Stolos, please initialize it."
        " You probably just want to call stolos.api.initialize()'")

    def __getattr__(self, *args, **kwargs):
        raise Uninitialized(Uninitialized.msg)

    def __repr__(self):
        return "Stolos Not Initialized.  %s" % Uninitialized.msg

    def __str__(self):
        return repr(self)


def get_NS():
    """Returns a namespace containing configuration variables.  Stolos must be
    initialized before NS is set.  This ensures that relevant configuration is
    properly defined.

    Users of stolos can just call stolos.api.initialize()

    Developers of stolos need to ensure that either they are using the api or,
    for if developing on internals, that stolos.initializer.initialize(...) is
    called for the the module(s) you are working on.  Keep in mind that only
    the api and Stolos's runner.py should initialize Stolos normally
    """
    try:
        return NS
    except NameError:
        raise Uninitialized(Uninitialized.msg)


__all__ = ['api']
