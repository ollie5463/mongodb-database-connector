#! /bin/bash

mongoimport --host "$1":"$2" --db users --collection profiles --type json --file ./tests/test_data/profiles.json --jsonArray
