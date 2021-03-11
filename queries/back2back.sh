#!/bin/bash

for query in *sql;
do
	echo "Running $query"
	base="${query%.sql}"
	result="b2bdata/${base}-result.tsv"
	expected="b2bdata/${base}-expected.tsv"
	./run.sh "$query" | sort > "$result"
	diff -q "${result}" "${expected}" || {
		failed="$failed $base"
	}
done
for a in $failed;
do
	echo -e "\033[31mFailed $a\033[0m"
done

[ "$failed" ] || echo -e "\033[32mAll passed\033[0m" 


