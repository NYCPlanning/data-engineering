case $1 in 
    dataloading ) ./ingest/data_library/dataloading.sh;;
    build ) 
        case $2 in 
            census ) python3 -m external_review.collate_save_census $3 $4;;
            category ) python3 -m external_review.external_review_collate $3 $4 $5;;
            * ) (python3 -m external_review.collate_save_census; python3 -m external_review.external_review_collate);;
        esac;;
    export ) ./external_review/export_DO.sh $2;;
    * ) echo "$@ command not found";
esac
