#!/bin/bash
function numbldgs_geocode {
   cd python
   python3 numbldgs.py
   cd ..
   mc cp $(pwd)/python/pluto_input_numbldgs.csv spaces/edm-recipes/tmp/pluto_input_numbldgs.csv
}
register 'geocode' 'numbldgs' 'geocode numbldgs' numbldgs_geocode

function clean_numbldgs {
   rm $(pwd)/python/pluto_input_numbldgs.csv
   mc rm spaces/edm-recipes/tmp/pluto_input_numbldgs.csv
}
register 'clean' 'numbldgs' 'geocode numbldgs' clean_numbldgs
