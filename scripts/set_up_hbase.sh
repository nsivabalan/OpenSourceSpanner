mkdir ~/hbase_setup
cd ~/hbase_setup
cp ~/OpenSourceSpanner/lib/hbase-0.94.17.tar.gz .
tar xfvz hbase-0.94.17.tar.gz
echo "export JAVA_HOME=/usr/lib/jvm/java-7-oracle/jre/" >> hbase-0.94.17/conf/hbase-env.sh
./hbase-0.94.17/bin/start-hbase.sh
echo "create 'userTable', 'cf'" | ./hbase-0.94.17/bin/hbase shell

