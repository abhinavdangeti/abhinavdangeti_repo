A Bloom Filter

1. Edit calc.c and set the following:
    - estimated_key_count (n)
    - allowed_probability_for_false_positives (p)

2. Compile & Run calc.c as:
    make calculate TESTARGS="<key_count> <false_postive_probability>"

3. Compile & Run theFilter.cc as:
    make estimate TESTARGS="<bitArray_size> <hashes_count>"
