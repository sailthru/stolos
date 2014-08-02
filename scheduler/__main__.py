from runner import build_arg_parser, main


def go():
    NS = build_arg_parser()
    main(NS)

if __name__ == '__main__':
    go()
