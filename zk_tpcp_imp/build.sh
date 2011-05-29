p=$PWD
cd ../zk_tpcp/
./build.sh 
cd $p

rm -fr bin/*
javac -d bin -cp "lib/*" -sourcepath src src/zk_tpcp/imp/*.java
