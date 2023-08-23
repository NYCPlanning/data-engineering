#!/bin/bash

function setup {
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh
    echo "$SSH_PRIVATE_KEY_GINGER" > ~/.ssh/ginger
    chmod 600 ~/.ssh/ginger
    echo "Host $GINGER_HOST
  User $GINGER_USER
  PubkeyAcceptedAlgorithms +ssh-rsa
  HostkeyAlgorithms +ssh-rsa" >> ~/.ssh/config
    chmod 600 ~/.ssh/config
}
register 'ssh' 'setup' 'ssh setup' setup 

function ssh_ls {
    sftp -i ~/.ssh/ginger \
    -F ~/.ssh/config \
    -o StrictHostKeyChecking=no \
    $GINGER_USER@$GINGER_HOST << EOF
    ls $@
EOF
}
register 'ssh' 'ls' 'ssh ls' ssh_ls 

function ssh_cmd {
    sftp -i ~/.ssh/ginger \
    -F ~/.ssh/config \
    -o StrictHostKeyChecking=no \
    $GINGER_USER@$GINGER_HOST << EOF
    $@
EOF
}
register 'ssh' 'cmd' 'ssh cmd' ssh_cmd 

function download_file {
    case $1 in
        pts )
            local file=PTS_Propmast.gz
        ;;
        cama)
            local file=CAMA.zip
        ;;
        *)
            echo "pts and cama are only options for pluto ssh download"
            exit 1
        ;;
    esac
    local folder=/tmp/$1/
    mkdir -p $folder && (
        cd $folder
        ssh_cmd get Prod_FromDOF/$file
        ls
    )
    if [[ ! -f $folder/$file ]]; then
        exit 2
    fi
}
register 'ssh' 'download' 'ssh download' download_file 
