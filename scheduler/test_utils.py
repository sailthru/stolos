def configure_logging(logname):
    import logging
    from colorlog import ColoredFormatter

    _ignore_log_keys = set(logging.makeLogRecord({}).__dict__)

    def _json_format(record):
        extras = ' '.join(
            "%s=%s" % (k, record.__dict__[k])
            for k in set(record.__dict__).difference(_ignore_log_keys))
        if extras:
            record.msg = "%s    %s" % (record.msg, extras)
        return record

    class ColoredJsonFormatter(ColoredFormatter):
        def format(self, record):
            record = _json_format(record)
            return super(ColoredJsonFormatter, self).format(record)

    log = logging.getLogger('scheduler')
    _formatter = ColoredJsonFormatter(
        "%(log_color)s%(levelname)-8s %(message)s %(reset)s %(cyan)s",
        reset=True)
    _h = logging.StreamHandler()
    _h.setFormatter(_formatter)
    log.handlers = [_h]  # log.addHandler(_h)
    log.setLevel(logging.DEBUG)
    return logging.getLogger(logname)
