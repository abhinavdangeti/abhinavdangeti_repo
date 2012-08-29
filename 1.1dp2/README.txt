README:
Instructions:

- Manually set up a cluster, create a bucket.
- Query a view

- test.properties
	item-count
	bucket-name
	bucket-password
	expiration
	ratio-expires
	servers
	port
	do-delete (flag)
	ratio-deletes
	ddoc-name
	view-name

- make load
- make view
- make clean

- LOADTASK:
	- Compiling:
		javac -cp :couchbase-client-1.1-dp2.jar:couchbase-client-1.1-dp2-javadocs.jar:spymemcached-2.8.4.jar:spymemcached-2.8.4-javadocs.jar:commons-codec-1.5.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:httpclient-cache-4.2.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar LoadTask.java
				
	- Running:
		java -cp .:couchbase-client-1.1-dp2.jar:couchbase-client-1.1-dp2-javadocs.jar:spymemcached-2.8.4.jar:spymemcached-2.8.4-javadocs.jar:commons-codec-1.5.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:httpclient-cache-4.2.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar LoadTask
		
- VIEWQUERY
	- Compiling:
		javac -cp :couchbase-client-1.1-dp2.jar:couchbase-client-1.1-dp2-javadocs.jar:spymemcached-2.8.4.jar:spymemcached-2.8.4-javadocs.jar:commons-codec-1.5.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:httpclient-cache-4.2.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar ViewQuery.java
	
	- Running:
		java -cp .:couchbase-client-1.1-dp2.jar:couchbase-client-1.1-dp2-javadocs.jar:spymemcached-2.8.4.jar:spymemcached-2.8.4-javadocs.jar:commons-codec-1.5.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:httpclient-cache-4.2.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar ViewQuery
