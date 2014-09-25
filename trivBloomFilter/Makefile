calculate:
	g++ theCalc.cc -o theCalc && ./theCalc $(TESTARGS) && rm -f theCalc

estimate:
	g++ -o theFilter theFilter.cc MurmurHash3.cc && ./theFilter $(TESTARGS) && rm -f theFilter

clean:
	rm -f theCalc MurmurHash3 theFilter
