WORKSPACE=~/eclipse/workspace/OpenSourceSpanner/
WORKSPACE1=~/eclipse/workspace/PaxosCP3/
echo "EXPERIMENT ___________"

if [ $# -lt 3 ]
then
	echo -e "USAGE: $0 [target] [field-count] [thread-count] -Dpcp.extended [true/false] {server/ports}"
	echo -e "\n\ttarget: number of transactions per second for all threads"
	echo -e "\tfield-count: number of attributes"
	echo -e "\tpaxos-type: extended or basic"
	echo -e "\t{server/ports}: ordered triplets: -Dpcp.repX server-name -Dpcp.cPortX port1 -Dpcp.repPort port2"
	exit
fi

#t=`expr $# % 3`
#if [ $t -ne 0 ]
#then
#	echo "should enter triplets"
#	exit
#fi

target=$1 # overall txn per sec
shift
fieldcount=$1
shift
threadcount=$1
shift
#type=$1 # extended or basic paxos
#shift

#echo $type > $ccfactory

#sleep 3

cp $WORKSPACE/conf/benchmark.properties.BAK $WORKSPACE/conf/benchmark.properties

echo fieldcount=$fieldcount >> $WORKSPACE/conf/benchmark.properties
echo threadcount=$threadcount >> $WORKSPACE/conf/benchmark.properties
echo readonly=0.1 >> $WORKSPACE/conf/benchmark.properties

echo "RUN Experiment: $* ${target} ${fieldcount}"


/usr/bin/java -Xmx500m $* -Dlog4j.configuration=my.properties -Djava.library.path=/usr/local/lib -classpath $WORKSPACE/dist/osspanner.jar:$WORKSPACE/bin/:$WORKSPACE/:$WORKSPACE/lib/commons-cli-1.1.jar:$WORKSPACE/lib/commons-configuration-1.6.jar:$WORKSPACE/lib/commons-io-1.2.jar:$WORKSPACE/lib/commons-lang-2.5.jar:$WORKSPACE/lib/commons-logging-1.1.1.jar:$WORKSPACE/lib/gson-2.2.2.jar:$WORKSPACE/lib/hadoop-core-1.0.4.jar:$WORKSPACE/lib/hbase-0.94.5.jar:$WORKSPACE/lib/log4j-1.2.16.jar:$WORKSPACE/lib/protobuf-java-2.4.0a.jar:$WORKSPACE/lib/zookeeper-3.4.5.jar:/usr/local/share/java/zmq.jar:$WORKSPACE/lib/slf4j-api-1.7.6.jar:/usr/lib/tools.jar:$WORKSPACE1/Benchmark-YCSB/conf:$WORKSPACE1/Benchmark-YCSB/build/ycsb.jar:$WORKSPACE1/Benchmark-YCSB/lib/commons-logging-1.0.4.jar:$WORKSPACE1/Benchmark-YCSB/lib/commons-logging-api-1.0.4.jar:$WORKSPACE1/Benchmark-YCSB/lib/hadoop-core-1.0.0.jar:$WORKSPACE1/Benchmark-YCSB/lib/jackson-core-asl-1.5.2.jar:$WORKSPACE1/Benchmark-YCSB/lib/jackson-mapper-asl-1.5.2.jar:$WORKSPACE1/Benchmark-YCSB/lib/log4j-1.2.15.jar:$WORKSPACE1/Benchmark-YCSB/db/cassandra-0.5/lib/*.jar:$WORKSPACE1/Benchmark-YCSB/db/jdbcdb/lib/h2.jar com.yahoo.ycsb.Client -t -db spanner.node.UserYCSBClient -target $target -P $WORKSPACE/conf/benchmark.properties
 
