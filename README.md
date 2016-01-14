spark-tests
===============

This repo contains an example app reproducing spark connector bug detailed [here](https://datastax-oss.atlassian.net/browse/SPARKC-264)


Installation prerequisites
-------

To run these tests, you need:
- JDK 8 or greater
- Git
- Maven

Download the project
-------

git clone https://github.com/jsebrien/test-spark-connector.git

How to reproduce
-------

Just execute the following command:<br/><br/>
mvn  clean package -DskipTests exec:java

This will start an embedded cassandra server, an embedded spark 1.5.2 cluster, accessed by Spark Cassandra 1.5.0-RC1 connector.

When creating a JavaRDD instance, from specific columns in a cassandra table, it will throw the following error:

Caused by: com.datastax.driver.core.exceptions.SyntaxError: line 1:8 no viable alternative at input 'FROM' (SELECT  [FROM]...)

