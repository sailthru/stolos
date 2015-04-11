"""
Convert a directory into an executable
"""
from stolos.runner import build_arg_parser_and_parse_args, main


def go():
    NS = build_arg_parser_and_parse_args()
    main(NS)

if __name__ == '__main__':
    go()
