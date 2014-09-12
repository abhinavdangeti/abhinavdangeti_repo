A Bloom Filter

1. Compile & Run theCalc.cc as:
    make calculate TESTARGS="<key_count> <false_postive_probability>"

    This would output bitArray_size and hashes_count, which are needed to compile and run theFilter.

2. Compile & Run theFilter.cc as:
    make estimate TESTARGS="<bitArray_size> <hashes_count>"
