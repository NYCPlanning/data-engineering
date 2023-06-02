#!/bin/bash
function ssh_ls {
    sftp -q -i ~/.ssh/ginger \
    -o StrictHostKeyChecking=no \
    $GINGER_USER@$GINGER_HOST << EOF
    ls $@
EOF
}
register 'ssh' 'ls' 'ssh ls' ssh_ls 

function ssh_cmd {
    sftp -q -i ~/.ssh/ginger \
    -o StrictHostKeyChecking=no \
    $GINGER_USER@$GINGER_HOST << EOF
    $@
EOF
}
register 'ssh' 'cmd' 'ssh cmd' ssh_cmd 
