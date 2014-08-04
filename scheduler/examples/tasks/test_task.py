"""
Test that tasks get executed properly using zookeeper
"""
from scheduler import argparse_shared as at
import logging
log = logging.getLogger('scheduler.examples.test_task')


def main(sc, ns, **job_id_identifiers):
    if ns.disable_log:
        logging.disable = True
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
