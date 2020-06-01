#!/bin/bash

./list.sh | while read v; do ./export.sh $v; done

