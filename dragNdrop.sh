#!/bin/bash 

cd `dirname "$0"`
echo "What's the threshold for ignore? (in cM)"
read cM
echo "What's the minimum SNPs in one segment?"
read snps
echo "What's the output file prefix? (no spaces)"
read prefix
node run.js $cM $snps $prefix $@
echo "Job finished, enter for close."
read ends