README:
Instructions:

- Manually set up a cluster, create a bucket.
- Load, add, replace, delete
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
	do-replace (flag)
	replace-ratio
	do-add (flag)
	add-ratio
	ddoc-name
	view-name

- Mainhelper:
	- Compiling:
		javac -cp :commons-codec-1.5.jar:couchbase-client-1.1.0c.jar:spymemcached-2.8.3-RAGS-SNAPSHOT.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar Mainhelper.java 
				
	- Running:
		java -cp .:commons-codec-1.5.jar:couchbase-client-1.1.0c.jar:spymemcached-2.8.3-RAGS-SNAPSHOT.jar:httpcore-4.1.1.jar:httpcore-nio-4.1.1.jar:jettison-1.1.jar:netty-3.2.0.Final.jar Mainhelper
