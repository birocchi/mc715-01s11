javac -d bin -cp "lib/*" -g src/org/apache/zookeeper/recipes/tpcp/*
jar cf zk_tpcp.jar -C bin/ .

