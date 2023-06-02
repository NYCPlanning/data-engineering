#!/bin/bash
function numbldgs_geocode {
   docker run --rm\
        --user $(id -u):$(id -g)\
        -v $(pwd)/python:/home/python\
        -w /home/python\
        -e RECIPE_ENGINE=$RECIPE_ENGINE\
        -e API_TOKEN=$API_TOKEN\
       nycplanning/docker-geosupport:latest python3 numbldgs.py
   mc cp $(pwd)/python/pluto_input_numbldgs.csv spaces/edm-recipes/tmp/pluto_input_numbldgs.csv
}
register 'geocode' 'numbldgs' 'geocode numbldgs' numbldgs_geocode

function clean_numbldgs {
   rm $(pwd)/python/pluto_input_numbldgs.csv
   mc rm spaces/edm-recipes/tmp/pluto_input_numbldgs.csv
}
register 'clean' 'numbldgs' 'geocode numbldgs' clean_numbldgs
