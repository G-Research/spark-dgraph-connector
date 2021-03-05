echo "===== missing commits ====="
diff <(git log --pretty=format:'%s' spark-2.4..spark-3.1; echo) <(git log --pretty=format:'%s' spark-3.1..spark-2.4; echo) | grep "^[<>]" | sed -e "s/^</spark-2.4:/" -e "s/^>/spark-3.1:/"

echo
echo "===== spark-3.1 ====="
git log --max-count=10 --graph --pretty=format:'%Cred%h%Creset - %s%C(yellow)%d%Creset %Cgreen(%cr)%Creset' --abbrev-commit --date=relative spark-2.4..spark-3.1

echo
echo "===== spark-2.4 ====="
git log --max-count=10 --graph --pretty=format:'%Cred%h%Creset - %s%C(yellow)%d%Creset %Cgreen(%cr)%Creset' --abbrev-commit --date=relative spark-3.1..spark-2.4

