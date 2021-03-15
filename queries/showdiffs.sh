#!/bin/bash
for a in b2bdata/*result.tsv; do vimdiff $a ${a/-result/-expected}; done
