#!/bin/bash 

cd `dirname "$0"`
echo "Whats the threshold for ignore? (in cM)"
read cM
node run.js $cM $@
echo "Job finished, enter for close."
read ends