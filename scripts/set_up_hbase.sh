wget http://mirror.metrocast.net/apache/hbase/stable/hbase-0.94.16.tar.gz
tar xfvz hbase-0.94.16.tar.gz
echo "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre/" >> hbase-0.94.16/conf/hbase-env.sh
./hbase-0.94.16/bin/start-hbase.sh
echo "create 'userTable', 'cf'" | ./hbase-0.94.16/bin/hbase shell

