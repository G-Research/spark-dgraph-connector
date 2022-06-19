base=spark-3.3
branch=spark-3.2

echo "===== missing commits ====="
diff <(git log --pretty=format:'%s' $branch..$base; echo) <(git log --pretty=format:'%s' $base..$branch; echo) | grep "^[<>]" | sed -e "s/^</$branch:/" -e "s/^>/$base:/"

echo
echo "===== $base ====="
git log --max-count=10 --graph --pretty=format:'%Cred%h%Creset - %s%C(yellow)%d%Creset %Cgreen(%cr)%Creset' --abbrev-commit --date=relative $branch..$base

echo
echo "===== $branch ====="
git log --max-count=10 --graph --pretty=format:'%Cred%h%Creset - %s%C(yellow)%d%Creset %Cgreen(%cr)%Creset' --abbrev-commit --date=relative $base..$branch

