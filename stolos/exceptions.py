from stolos import log


def _log_raise_if(cond, msg, extra, exception_kls):
    if cond:
        _log_raise(msg, extra, exception_kls)


def _log_raise(msg, extra, exception_kls):
    log.error(msg, extra=extra)
    raise exception_kls(msg)


class StolosException(Exception):
    """Base Class for all Stolos Exceptions"""
    pass


class NoNodeError(StolosException):
    pass


class NodeExistsError(StolosException):
    pass


class CodeError(StolosException):
    pass


class JobAlreadyQueued(StolosException):
    pass


class CouldNotObtainLock(StolosException):
    pass


class LockAlreadyAcquired(StolosException):
    pass


class DAGMisconfigured(StolosException):
    pass


class InvalidJobId(StolosException):
    pass
