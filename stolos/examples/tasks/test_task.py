"""
Test that tasks get executed properly using zookeeper
"""
from stolos import argparse_shared as at
from stolos.examples import log


def main(sc, ns, **job_id_identifiers):
    if ns.disable_log:
        import logging
        logging.disable = True
    log.info(ns.read_fp)
    log.info('test_module!!!')
    log.info('default ns: %s' % ns)
    if ns.fail:
        raise Exception("You asked me to fail, so here I am!")


build_arg_parser = at.build_arg_parser([at.group(
    "Test spark task",
    at.add_argument('--fail', action='store_true'),
    at.add_argument('--disable_log', action='store_true'),

)], conflict_handler='resolve'
)
