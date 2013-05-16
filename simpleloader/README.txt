Sets / Gets 

Edit test.properties for run-spec and cluster details

- Gets run throughout
- Sets run as part of phase1 (just the phase 1 here)

test.properties:
- json:false   		... Doesn't generate JSON documents
- item-count   		... no. of sets in phase1
- items-size   		... document size for sets in phase1 and phase2 in bytes
- prefix                ... Prefix for generated keys

To compile:
make Loadrunner

To run:
make RunLoadrunner
