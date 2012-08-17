README:
Instructions:

Manually set up a cluster, create a bucket.
Create a view

- Set the server name, bucket name/password, viewname etc. in test.properties.

- LOADTASK:
	- Compiling:
		javac -cp :couchbase-client-1.1-dp-observe-preview4-javadocs.jar:
				couchbase-client-1.1-dp-observe-preview4-sources.jar:
				couchbase-client-1.1-dp-observe-preview4.jar:
				spymemcached-2.8.3-SNAPSHOT-javadocs.jar:
				spymemcached-2.8.3-SNAPSHOT-sources.jar:
				spymemcached-2.8.3-SNAPSHOT.jar:
				httpcore-4.2.1.jar:
				httpcore-nio-4.1.1.jar:
				netty-3.2.0.Final.jar:
				jettison-1.1.jar 
				LoadTask.java
				
	- Running:
		java -cp .:couchbase-client-1.1-dp-observe-preview4-javadocs.jar:
				couchbase-client-1.1-dp-observe-preview4-sources.jar:
				couchbase-client-1.1-dp-observe-preview4.jar:
				spymemcached-2.8.3-SNAPSHOT-javadocs.jar:
				spymemcached-2.8.3-SNAPSHOT-sources.jar:
				spymemcached-2.8.3-SNAPSHOT.jar:
				httpcore-4.2.1.jar:
				httpcore-nio-4.1.1.jar:
				netty-3.2.0.Final.jar:
				jettison-1.1.jar:
				commons-codec-1.5.jar
				LoadTask
		
- VIEWQUERY
	- Compiling:
		javac -cp :couchbase-client-1.1-dp-observe-preview4-javadocs.jar:
				couchbase-client-1.1-dp-observe-preview4-sources.jar:
				couchbase-client-1.1-dp-observe-preview4.jar:
				spymemcached-2.8.3-SNAPSHOT-javadocs.jar:
				spymemcached-2.8.3-SNAPSHOT-sources.jar:
				spymemcached-2.8.3-SNAPSHOT.jar:
				httpcore-4.2.1.jar:
				httpcore-nio-4.1.1.jar:
				netty-3.2.0.Final.jar:
				jettison-1.1.jar:
				httpclient-cache-4.2.1.jar 
				ViewQuery.java
	- Running:
		java -cp .:couchbase-client-1.1-dp-observe-preview4-javadocs.jar:
				couchbase-client-1.1-dp-observe-preview4-sources.jar:
				couchbase-client-1.1-dp-observe-preview4.jar:
				spymemcached-2.8.3-SNAPSHOT-javadocs.jar:
				spymemcached-2.8.3-SNAPSHOT-sources.jar:
				spymemcached-2.8.3-SNAPSHOT.jar:
				httpcore-4.2.1.jar:
				httpcore-nio-4.1.1.jar:
				netty-3.2.0.Final.jar:
				jettison-1.1.jar:
				commons-codec-1.5.jar:
				httpclient-cache-4.2.1.jar
				ViewQuery