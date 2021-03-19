tar -xzf apache-zookeeper-3.6.2-bin.tar.gz
mv apache-zookeeper-3.6.2 zookeeper
cp zookeeper/conf/zoo_sample.cfg zookeeper/conf/zoo.cfg 
zookeeper/bin/zkServer.sh start
telnet 127.0.0.1:2181
