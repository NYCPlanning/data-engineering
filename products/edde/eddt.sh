case $1 in
    build ) python3 -m build $2 $3 $4;;
    qa ) python3 -m qa.proto_qa;;
    package ) python3 -m packager;;
    * ) echo "$@ command not found";
esac
