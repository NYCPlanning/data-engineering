case $1 in
    dataloading ) ./ingest/data_library/dataloading.sh;;
    build ) python3 -m build $2 $3 $4;;
    qa ) python3 -m qa.proto_qa;;
    package ) python3 -m packager;;
    * ) echo "$@ command not found";
esac
