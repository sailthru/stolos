from ds_commons.log import log


def _log_raise_if(cond, msg, extra, exception_kls):
    if cond:
        _log_raise(msg, extra, exception_kls)


def _log_raise(msg, extra, exception_kls):
    log.error(msg, extra=extra)
    raise exception_kls(msg)


class CodeError(Exception):
    pass


class JobAlreadyQueued(Exception):
    pass


class CouldNotObtainLock(Exception):
    pass


class LockAlreadyAcquired(Exception):
    pass


class DAGMisconfigured(Exception):
    pass


class InvalidJobId(Exception):
    pass
