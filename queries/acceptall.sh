#!/bin/bash
for a in b2bdata/*result.tsv; do mv $a ${a/-result/-expected}; done
