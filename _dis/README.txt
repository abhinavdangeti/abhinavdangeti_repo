Sets / Gets / Appends

Edit test.properties for run-spec and cluster details

- Gets run throughout
- Sets run as part of phase1
- Additional sets and appends follow as part of phase2
- Test runs until conditions specified in test.properties are satisfied

test.properties:
- json:false   		... Doesn't generate JSON documents
- item-count   		... no. of sets in phase1
- items-size   		... document size for sets in phase1 and phase2 in bytes
- items-to-add     	... no. of sets in phase2
- append-count		... no. of times to append
- append-ratio		... ratio of items set in phase1 to be appended
- append-data-size  	... data appended every time in bytes
- prefix                ... Prefix for generated keys

To compile:
make Loadrunner

To run:
make RunLoadrunner

To clean:
make clean
