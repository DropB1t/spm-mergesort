#!/bin/zsh
# Compare generated.txt and sorted.txt for correct sorting by key

nl -ba generated.txt | sort -ns -k2,2 -k1,1 - | cut -f2 - > generated_sorted.txt
#sort -s -k1,1 generated.txt > generated_sorted.txt

diff -u generated_sorted.txt sorted.txt > diff.txt

if [ $? -eq 0 ]; then
    echo "SUCCESS: The mergesort output matches the sorted generated records."
else
    echo "FAIL: The mergesort output does not match the sorted generated records. See diff.txt."
fi
