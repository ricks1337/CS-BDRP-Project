run_container:
	docker run --volume $(shell pwd):/myhome --rm --interactive --tty uwsampa/giraph-docker
compile:
	$(eval containerid := $(shell docker ps -a -q))
	$(eval cpath := $(shell docker exec -it $(containerid) /usr/local/hadoop/bin/hadoop classpath))
	docker exec -it $(containerid) bash -c "cd /myhome/ && javac -cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-2.4.1-jar-with-dependencies.jar:$(cpath) code/DummyComputation.java && cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-2.4.1-jar-with-dependencies.jar ./myjar.jar && jar uf myjar.jar code"

data_hdfs:
	$(eval containerid := $(shell docker ps -a -q))
	$(eval filename := "test.txt")
	docker exec -it $(containerid) bash -c "cd /myhome/ && /usr/local/hadoop/bin/hdfs dfs -put $(filename) ./$(filename)"

run:
	$(eval containerid := $(shell docker ps -a -q))
	docker exec -it $(containerid) bash -c "cd /myhome/ && /usr/local/hadoop/bin/hadoop jar myjar.jar org.apache.giraph.GiraphRunner code.DummyComputation --yarnjars myjar.jar --workers 1 --vertexInputFormat org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat --vertexInputPath ./tiny-graph.txt -vertexOutputFormat org.apache.giraph.io.formats.IdWithValueTextOutputFormat --outputPath /user/root/dummy-output"

#podemos cambiar el directorio de output en hdfs
result:
	$(eval containerid := $(shell docker ps -a -q))
	docker exec -it $(containerid) bash -c "/usr/local/hadoop/bin/hdfs dfs -cat /user/root/dummy-output/*"