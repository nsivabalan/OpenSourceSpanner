package spanner.metadataservice;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import spanner.common.Common;
import spanner.message.PaxosDetailsMsg;
import spanner.protos.*;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementToServerMapping;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.PartitionServerElementProto;
import spanner.protos.Protos.PartitionServerProto;
import spanner.protos.Protos.ElementsSetProto.ElementsSetTypeProto;
import spanner.protos.Protos.TransactionMetaDataProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

public class MetaDS {

	HashMap<String, NodeProto> serverAddressMap ;
	HashMap<String, NodeProto> shardToParticipantMap;
	HashMap<String, ArrayList<NodeProto>> shardToAcceptorsMap;
	ArrayList<String> orderedShards = new ArrayList<String>();

	public MetaDS() throws IOException
	{
		serverAddressMap = new HashMap<String,NodeProto>();
		shardToParticipantMap = new HashMap<String, NodeProto>();
		shardToAcceptorsMap = new HashMap<String, ArrayList<NodeProto>>();
		String[] shards = Common.getProperty("shards").split(",");
		String paxosLog = Common.PaxosLog;
		File paxosLogFile = new File(paxosLog);
		if(! paxosLogFile.exists())
			paxosLogFile.mkdir();
		
		for(String shard: shards)
		{
			File tempPartition = new File(paxosLog+"/"+shard);
			if( !tempPartition.exists())
				tempPartition.mkdir();
			String[] shardDetails = Common.getProperty(shard).split(";");
			String shardLeader = shardDetails[0];
			String[] acceptors = shardDetails[1].split(",");
			shardToAcceptorsMap.put(shard, new ArrayList<NodeProto>());
			for(String acceptor: acceptors)
			{
				File tempAcceptor = new File(tempPartition+"/"+acceptor+"_.log");
				if( !tempAcceptor.exists())
					tempAcceptor.createNewFile();
				
				String[] hostdetail = Common.getProperty(acceptor).split(":");
				if(hostdetail[0].equalsIgnoreCase("localhost")){
					NodeProto tempNode = NodeProto.newBuilder().setHost("127.0.0.1").setPort(Integer.parseInt(hostdetail[1])).build();
					serverAddressMap.put(acceptor, tempNode);
					shardToAcceptorsMap.get(shard).add(tempNode);
				}
				else{
					NodeProto tempNode = NodeProto.newBuilder().setHost(hostdetail[0]).setPort(Integer.parseInt(hostdetail[1])).build();
					serverAddressMap.put(acceptor, tempNode);
					shardToAcceptorsMap.get(shard).add(tempNode);
				}
			}
			//shardToParticipantMap.put(shard, serverAddressMap.get(shardLeader));
			shardToParticipantMap.put(shard, null);
			orderedShards.add(shard);
		}
	}


	/*final class TransactionState {

		String uid ;
		Common.TransactionType transState;
		HashMap<String, ArrayList<String>> readSet;
		HashMap<String, HashMap<String, String>> writeSet;
		HashMap<NodeProto, HashMap<String, ArrayList<String>> > readSetServerInfoMap;
		HashMap<NodeProto, HashMap<String, HashMap<String, String>> > writeSetServerInfoMap;
		String clientRoutingKey;
		HostAddress twoPCAddress;


		public TransactionState(String uid,HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
		{			
			this.uid = uid;
			readSet = new HashMap<String, ArrayList<String>>();
			writeSet = new HashMap<String, HashMap<String, String>>();
			readSetServerInfoMap = new HashMap<NodeProto, HashMap<String, ArrayList<String>> >();
			writeSetServerInfoMap = new HashMap<NodeProto, HashMap<String, HashMap<String, String>>>();
			this.writeSet = writeSet;
			this.readSet = readSet;
			for(String key: readSet.keySet())
			{
				NodeProto tempAddress = getNodeAddress(key);
				if(readSetServerInfoMap.containsKey(tempAddress))
				{
					readSetServerInfoMap.get(tempAddress).put(key, readSet.get(key));
				}
				else{
					HashMap<String, ArrayList<String>> tempRecord = new HashMap<String, ArrayList<String>>();
					tempRecord.put(key, readSet.get(key));
					readSetServerInfoMap.put(tempAddress, tempRecord);
				}
			}

			for(String key: writeSet.keySet())
			{
				NodeProto tempAddress = getNodeAddress(key);
				if(writeSetServerInfoMap.containsKey(tempAddress))
				{
					writeSetServerInfoMap.get(tempAddress).put(key, writeSet.get(key));
				}
				else{
					HashMap<String, HashMap<String, String>> tempRecord = new HashMap<String, HashMap<String, String>>();
					tempRecord.put(key, writeSet.get(key));
					writeSetServerInfoMap.put(tempAddress, tempRecord);
				}
			}
		}			
	}*/

	static class HostAddress{
		String hostname;
		int port;

		public HostAddress(String hostname, int port)
		{
			this.hostname = hostname;
			this.port = port;
		}
	}

	public NodeProto getLeaderAddress(String shard)
	{	
			return shardToParticipantMap.get(shard);
	}
	
	public void setLeaderAddress(String shard, NodeProto leader)
	{
		shardToParticipantMap.put(shard, leader);
	}
	
	private NodeProto getNodeAddress(String rowKey)
	{
		int hash = (rowKey.charAt(0) - 'a')% shardToParticipantMap.size();
		return shardToParticipantMap.get(orderedShards.get(hash));
	}

	public NodeProto getShardLeader(String shardLeader)
	{
		return shardToParticipantMap.get(shardLeader);
		
	}
	
	public ArrayList<NodeProto> getAcceptors(String shardId)
	{
		return shardToAcceptorsMap.get(shardId);
	}
	
	public TransactionMetaDataProto getTransactionDetails(NodeProto clientAddress, HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet, String uid)
	{
		
		//TransactionState transaction = new TransactionState(uid, readSet, writeSet);
		ElementsSetProto.Builder readSetBuilder = ElementsSetProto.newBuilder();
		readSetBuilder.setElementsSetType(ElementsSetTypeProto.READSET);
		ElementToServerMapping.Builder readSetServerToRecordBuilder = ElementToServerMapping.newBuilder();
		HashMap<NodeProto, HashMap<String, ArrayList<String>>> readMap = new HashMap<NodeProto, HashMap<String, ArrayList<String>>>();
		HashMap<NodeProto, HashMap<String, HashMap<String, String>>> writeMap = new HashMap<NodeProto, HashMap<String, HashMap<String,String>>>();

		for(String key: readSet.keySet())
		{
			ElementProto.Builder elementBuilder = ElementProto.newBuilder();
			elementBuilder.setRow(key);
			for(String  col: readSet.get(key)){
				elementBuilder.addCols(ColElementProto.newBuilder().setCol(col).build());
			}
			readSetBuilder.addElements(elementBuilder.build());
			NodeProto shard = getNodeAddress(key);
			if(readMap.containsKey(shard))
			{
				readMap.get(shard).put(key, readSet.get(key));
			}
			else{
				HashMap<String, ArrayList<String>> temp = new HashMap<String, ArrayList<String>>();
				temp.put(key, readSet.get(key));
				readMap.put(shard, temp);
			}
		}

		ElementsSetProto.Builder writeSetBuilder = ElementsSetProto.newBuilder();
		writeSetBuilder.setElementsSetType(ElementsSetTypeProto.WRITESET);
		ElementToServerMapping.Builder writeSetServerToRecordBuilder = ElementToServerMapping.newBuilder();

		for(String key: writeSet.keySet())
		{
			ElementProto.Builder elementBuilder = ElementProto.newBuilder();
			elementBuilder.setRow(key);
			HashMap<String, String> colValues = writeSet.get(key);
			for(String  col: colValues.keySet()){
				elementBuilder.addCols(ColElementProto.newBuilder().setCol(col).setValue(colValues.get(col)).build());
			}
			writeSetBuilder.addElements(elementBuilder.build());
			NodeProto shard = getNodeAddress(key);
			if(writeMap.containsKey(shard))
			{
				writeMap.get(shard).put(key, writeSet.get(key));
			}
			else{
				HashMap<String, HashMap<String, String>> temp = new HashMap<String, HashMap<String, String>>();
				temp.put(key, writeSet.get(key));
				writeMap.put(shard, temp);
			}

		}

		
		for(NodeProto node: readMap.keySet())
		{
			HashMap<String, ArrayList<String>> reads = readMap.get(node);
			ElementsSetProto.Builder readMapBuilder = ElementsSetProto.newBuilder();
			for(String key: reads.keySet())
			{
				ElementProto.Builder elementBuilder = ElementProto.newBuilder();
				elementBuilder.setRow(key);
				for(String  col: readSet.get(key)){
					elementBuilder.addCols(ColElementProto.newBuilder().setCol(col).build());
				}
				readMapBuilder.addElements(elementBuilder.build());
			}
			PartitionServerProto shardProto = PartitionServerProto.newBuilder().setHost(node)
					.setPartition("dummy").build();
			readSetServerToRecordBuilder.addPartitionServerElement(PartitionServerElementProto.newBuilder()
					.setPartitionServer(shardProto).setElements(readMapBuilder.build()).build());
		}
		
		for(NodeProto node: writeMap.keySet())
		{
			HashMap<String, HashMap<String, String>> writes = writeMap.get(node);
			ElementsSetProto.Builder writeMapBuilder = ElementsSetProto.newBuilder();
			for(String key: writes.keySet())
			{
				ElementProto.Builder elementBuilder = ElementProto.newBuilder();
				elementBuilder.setRow(key);
				HashMap<String, String> colValues = writeSet.get(key);
				for(String  col: colValues.keySet()){
					elementBuilder.addCols(ColElementProto.newBuilder().setCol(col).setValue(colValues.get(col)).build());
				}
				writeMapBuilder.addElements(elementBuilder.build());
			}
			PartitionServerProto shardProto = PartitionServerProto.newBuilder().setHost(node)
					.setPartition("dummy").build();
			writeSetServerToRecordBuilder.addPartitionServerElement(PartitionServerElementProto.newBuilder()
					.setPartitionServer(shardProto).setElements(writeMapBuilder.build()).build());
		}



		NodeProto twoPC = null;
		if(writeSet.size() > 0)
		{
			int size = writeSet.size();
			int rand = new Random().nextInt(size);
			System.out.println("Random no chosen "+rand);
			System.out.println("Write set size "+size);
			Iterator<String> itr = writeSet.keySet().iterator();
			int count = 0;
			while(itr.hasNext())
			{
				if(rand == count)
				{
					twoPC = getNodeAddress(itr.next());
				}
				else
					itr.next();

				count++;

			}
			System.out.println("TPC chosen ****** "+twoPC);
			TransactionMetaDataProto transaction = TransactionMetaDataProto.newBuilder()
					.setTransactionID(uid)
					.setTransactionStatus(TransactionMetaDataProto.TransactionStatusProto.ACTIVE)
					.setReadSet(readSetBuilder.build())
					.setWriteSet(writeSetBuilder.build())
					.setReadSetServerToRecordMappings(readSetServerToRecordBuilder.build())
					.setWriteSetServerToRecordMappings(writeSetServerToRecordBuilder.build())
					.setTwoPC(twoPC)
					.build();

			return transaction;
		}
		else
		{
			TransactionMetaDataProto transaction = TransactionMetaDataProto.newBuilder()
					.setTransactionID(uid)
					.setTransactionStatus(TransactionMetaDataProto.TransactionStatusProto.ACTIVE)
					.setReadSet(readSetBuilder.build())
					.setWriteSet(writeSetBuilder.build())
					.setReadSetServerToRecordMappings(readSetServerToRecordBuilder.build())
					.setWriteSetServerToRecordMappings(writeSetServerToRecordBuilder.build())
					.build();

			return transaction;
		}

	}

}
