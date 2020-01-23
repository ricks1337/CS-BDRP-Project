#
compile:
	javac -cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-2.4.1-jar-with-dependencies.jar:$($HADOOP_HOME/bin/hadoop classpath) code/DummyComputation.java
	cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-2.4.1-jar-with-dependencies.jar ./myjar.jar
	jar uf myjar.jar code

#Hacer que sea nuestro archivo, realmente no sé donde está el tiny-graph.txt
data:
	$HADOOP_HOME/bin/hdfs dfs -put tiny-graph.txt /user/root/input/tiny-graph.txt

run:
	$HADOOP_HOME/bin/hadoop jar myjar.jar org.apache.giraph.GiraphRunner code.DummyComputation --yarnjars myjar.jar --workers 1 --vertexInputFormat org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat --vertexInputPath /user/root/input/tiny-graph.txt -vertexOutputFormat org.apache.giraph.io.formats.IdWithValueTextOutputFormat --outputPath /user/root/dummy-output

#podemos cambiar el directorio de output en hdfs
result:
	$HADOOP_HOME/bin/hdfs dfs -cat /user/root/dummy-output/*