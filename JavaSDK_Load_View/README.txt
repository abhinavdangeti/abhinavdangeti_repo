README:
Instructions:

Manually set up a cluster, create a bucket.
Create a view

- Set the server name, bucket name/password, viewname etc. in test.properties.
- LOADTASK:
	- Compiling:
		javac -cp couchbase-client-1.0.3.jar:spymemcached-2.8.1.jar LoadTask.java
	- Running:
		java -cp .:couchbase-client-1.0.3.jar:spymemcached-2.8.1.jar:jettison-1.1.jar:commons-codec-1.5.jar:netty-3.2.0.Final.jar LoadTask
		
- VIEWQUERY
	- Compiling:
		javac -cp couchbase-client-1.1-dp.jar:spymemcached-2.8.1.jar:commons-codec-1.5.jar:jettison-1.1.jar:netty-3.2.0.Final.jar ViewQuery.java
	- Running:
		java -cp .:couchbase-client-1.1-dp.jar:spymemcached-2.8.1.jar:commons-codec-1.5.jar:jettison-1.1.jar:netty-3.2.0.Final.jar:httpclient-4.2.1.jar:httpcore-4.2.1.jar:httpcore-nio-4.1.1.jar ViewQuery