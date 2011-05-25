rm -fr bin/*
javac -d bin -cp "lib/*" -sourcepath src src/zk_lock/imp/Main.java src/zk_lock/imp/client/Client.java src/zk_lock/imp/client/LockListner.java
