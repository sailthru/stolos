set -e
set -u

branch=$1
commit_msg=$2  # include things like "closes #issueNum"

echo "running: git checkout master"
git checkout master
git merge --ff-only --no-ff -m "merge branch: $branch.  $commit_msg" $branch
