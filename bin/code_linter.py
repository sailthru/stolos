#!/usr/bin/env python
"""
Syntax checker adapted from
[StarCluster](https://github.com/jtriley/StarCluster)

Strict PyFlakes and Pep8 checkers, with the following differences:
    - ignore UnusedImport and "Import *" errors from __init__.py files

"""
import os
import re
import ast
import sys
import glob
import subprocess

import pep8
from pyflakes import checker
from pyflakes.messages import UnusedImport, ImportStarUsed


def check(codeString, filename):
    """
    Check the Python source given by C{codeString} for flakes.

    @param codeString: The Python source to check.
    @type codeString: C{str}

    @param filename: The name of the file the source came from, used to report
        errors.
    @type filename: C{str}

    @return: The number of warnings emitted.
    @rtype: C{int}
    """
    # First, compile into an AST and handle syntax errors.
    try:
        tree = compile(codeString, filename, "exec", ast.PyCF_ONLY_AST)
    except SyntaxError, value:
        msg = value.args[0]

        (lineno, offset, text) = value.lineno, value.offset, value.text

        # If there's an encoding problem with the file, the text is None.
        if text is None:
            # Avoid using msg, since for the only known case, it contains a
            # bogus message that claims the encoding the file declared was
            # unknown.
            sys.stderr.write("%s: problem decoding source\n" % (filename, ))
        else:
            line = text.splitlines()[-1]

            if offset is not None:
                offset = offset - (len(text) - len(line))

            sys.stderr.write('%s:%d: %s' % (filename, lineno, msg))
            sys.stderr.write(line + '\n')

            if offset is not None:
                sys.stderr.write(" " * offset + "^\n")

        return 1
    else:
        # Okay, it's syntactically valid.  Now check it.
        w = checker.Checker(tree, filename)
        lines = codeString.split('\n')
        messages = [message for message in w.messages
                    if lines[message.lineno - 1].find('pyflakes:ignore') < 0]
        messages.sort(lambda a, b: cmp(a.lineno, b.lineno))
        false_positives = 0
        for warning in messages:
            if not (re.match('.*__init__.py', str(warning))
                    and isinstance(warning, (UnusedImport, ImportStarUsed))):
                print(warning)
            else:
                false_positives += 1
        return len(messages) - false_positives


def checkPath(filename):
    """
    Check the given path, printing out any warnings detected.

    @return: the number of warnings printed
    """
    try:
        return check(file(filename, 'U').read() + '\n', filename)
    except IOError, msg:
        sys.stderr.write("%s: %s\n" % (filename, msg.args[1]))
        return 1


def matches_file(file_name, match_files):
    return any(re.compile(match_file).match(file_name) for match_file in
               match_files)


def check_files(files, check):
    clean = True
    print check['start_msg']
    for file_name in files:
        if not matches_file(file_name, check.get('match_files', [])):
            continue
        if matches_file(file_name, check.get('ignore_files', [])):
            continue
        print 'checking file: %s' % file_name
        process = subprocess.Popen(check['command'] % file_name,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        output = out + err
        if output:
            output_lines = ['%s: %s' % (file_name, line) for line in
                            (out + err).splitlines()]
            print '\n'.join(output_lines)
        if process.returncode != 0:
            clean = False
    if not clean:
        raise Exception("ERROR: checks failed on some source files")


def find_py_files(path):
    for cfile in glob.glob(os.path.join(path, '*')):
        if os.path.isdir(cfile):
            for py in find_py_files(cfile):
                yield py
        if cfile.endswith('.py'):
            yield cfile


def check_pyflakes(files):
    print(">>> Running pyflakes...")
    clean = True
    for pyfile in files:
        if checkPath(pyfile) != 0:
            clean = False
    if not clean:
        raise Exception("ERROR: pyflakes failed on some source files")


def check_pep8(files):
    print(">>> Running pep8...")
    sg = pep8.StyleGuide(parse_argv=False, config_file=False)
    sg.options.repeat = True
    sg.options.show_pep8 = True
    report = sg.check_files(files)
    if report.total_errors:
        raise Exception("ERROR: pep8 failed on some source files")


def main(git_index=False, filetypes=['.py']):
    files = []
    if git_index:
        p = subprocess.Popen(['git', 'status', '--porcelain'],
                             stdout=subprocess.PIPE)
        out, err = p.communicate()
        modified = re.compile('^(?:MM|M|A)(\s+)(?P<name>.*)')
        for line in out.splitlines():
            match = modified.match(line)
            if match:
                f = match.group('name')
                if filetypes:
                    if f.endswith(tuple(filetypes)):
                        files.append(f)
                else:
                    files.append(f)
    else:
        src = os.path.join(os.path.dirname(__file__), '..')
        files = list(find_py_files(src))
    if not files:
        return
    try:
        check_pyflakes(files)
        check_pep8(files)
        print(">>> Clean!")
    except Exception, e:
        print
        print(e)
        print("ERROR: please fix the errors and re-run this script")
        sys.exit(1)

if __name__ == '__main__':
    git_index = '--git-index' in sys.argv
    main(git_index=git_index)
