DIR="$( dirname "$( cd "$( dirname "$0" )" && pwd )")"

$DIR/bin/test_stolos.sh --with-coverage --cover-html --cover-erase --cover-package=stolos --processes=
