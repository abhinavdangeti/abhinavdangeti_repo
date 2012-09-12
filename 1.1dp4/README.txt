README::
Instructions:

- Manually set up a cluster, create a bucket.
- Once bucket is created, and perhaps a view, update entries in test.properties.
	- test.properties
		item-count
		bucket-name
		observe (boolean: true - runs tests with observe)
		bucket-password
		expiration
		ratio-expires
		servers
		port
		do-delete (flag)
		ratio-deletes
		ddoc-name
		view-name
		do-replace (flag)
		replace-ratio
		do-add (flag)
		add-ratio
		ddoc-name
		view-name
		obs-poll-interval
		obs-poll-max

- Allowed operations:
	- Load
	- Add
	- Replace
	- Delete

- This test does sets and gets items asynchronously and then deletes, adds and replaces asynchronously and if a view has been set up, retrieves the content of the view or else simply outputs "result of load is false".
	
- Run Mainhelper to see results of the task at hand:
	- Mainhelper:
		- Compiling:
			javac -cp :couchbase-client-1.1.2c-TEST.jar:couchbase-client-1.1.2c-TEST-javadocs.jar:couchbase-client-1.1.2c-TEST-sources.jar:spymemcached-2.8.6-SNAPSHOT.jar:spymemcached-2.8.6-SNAPSHOT-javadocs.jar:spymemcached-2.8.6-SNAPSHOT-sources.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar:commons-codec-1.5.jar Mainhelper.java 
					
		- Running:
			java -cp .:couchbase-client-1.1.2c-TEST.jar:couchbase-client-1.1.2c-TEST-javadocs.jar:couchbase-client-1.1.2c-TEST-sources.jar:spymemcached-2.8.6-SNAPSHOT.jar:spymemcached-2.8.6-SNAPSHOT-javadocs.jar:spymemcached-2.8.6-SNAPSHOT-sources.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar:commons-codec-1.5.jar Mainhelper
