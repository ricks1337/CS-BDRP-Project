filename := ./data/soc-sign-bitcoinotc-Giraph.txt
filename_hdfs := soc-sign-bitcoinotc-Giraph.txt
java := src/*.java
java_name := LP
dir_hdfs := /user/root/lp-output6
run_container:
	docker run --volume $(shell pwd):/myhome --rm --interactive --tty uwsampa/giraph-docker
compile:
	$(eval containerid := $(shell docker ps -a -q))
	$(eval cpath := $(shell docker exec -it $(containerid) /usr/local/hadoop/bin/hadoop classpath))
	docker exec -it $(containerid) bash -c "cd /myhome/ && javac -cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-2.4.1-jar-with-dependencies.jar:$(cpath) $(java) && cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-2.4.1-jar-with-dependencies.jar ./myjar.jar && jar uf myjar.jar src"

data_hdfs:
	$(eval containerid := $(shell docker ps -a -q))
	docker exec -it $(containerid) bash -c "cd /myhome/ && /usr/local/hadoop/bin/hdfs dfs -put $(filename) ./$(filename_hdfs)"

run:
	$(eval containerid := $(shell docker ps -a -q))
	docker exec -it $(containerid) bash -c "cd /myhome/ && /usr/local/hadoop/bin/hadoop jar myjar.jar org.apache.giraph.GiraphRunner src.$(java_name) --yarnjars myjar.jar --workers 1 --vertexInputFormat src.LPVertexTextInputFormat --vertexInputPath ./$(filename_hdfs) -vertexOutputFormat src.LPVertexTextOutputFormat --outputPath $(dir_hdfs)"

#podemos cambiar el directorio de output en hdfs
result:
	$(eval containerid := $(shell docker ps -a -q))
	docker exec -it $(containerid) bash -c "/usr/local/hadoop/bin/hdfs dfs -cat $(dir_hdfs)/*"

download:
	$(eval containerid := $(shell docker ps -a -q))
	docker exec -it $(containerid) bash -c "/usr/local/hadoop/bin/hdfs dfs -get $(dir_hdfs)/* myhome/results/."
	docker exec -it $(containerid) bash -c "cp -r /usr/local/hadoop/logs/userlogs/* myhome/results/."
