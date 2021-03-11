#!/bin/bash
views=${@:-*sql}
for query in $views;
do
	echo "Running $query"
	base="${query%.sql}"
	result="b2bdata/${base}-result.tsv"
	expected="b2bdata/${base}-expected.tsv"
	rm -f "$result"
	./run.sh "$query" | sort > "$result"
	diff -q "${result}" "${expected}" && {
		rm -f "${result}"
	} || {
		failed="$failed $base"
	}
done
for a in $failed;
do
	echo -e "\033[31mFailed $a\033[0m"
done

[ "$failed" ] || echo -e "\033[32mAll passed\033[0m" 


