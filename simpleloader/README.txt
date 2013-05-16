Sets / Gets / Deletes

Edit test.properties for run-spec and cluster details

- Gets run throughout
- Sets run as part of phase1
- Deletes run in phase2

test.properties:
- json:false   		... Doesn't generate JSON documents
- item-count   		... no. of sets in phase1
- items-size   		... document size for sets in phase1 and phase2 in bytes
- prefix                ... Prefix for generated keys
- del-ratio             ... Ratio of item-count to be deleted

To compile:
make Loadrunner

To run:
make RunLoadrunner
