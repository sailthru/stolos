from subprocess import check_output


def run(cmd):
    cmd = "set -o pipefail ; %s" % cmd
    rv = check_output(cmd, shell=True, executable="bash")
    return rv


def test_stolos_submit():
    print(run("stolos-submit -h"))
    raise NotImplementedError()
