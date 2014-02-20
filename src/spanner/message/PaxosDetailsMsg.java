package spanner.message;

import java.util.ArrayList;
import java.util.HashMap;

import spanner.common.Common.MetaDataMsgType;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionMetaDataProto;
import spanner.protos.Protos.TransactionProto;

public class PaxosDetailsMsg extends MessageBase{

	private NodeProto source = null;
	private String shardID = null;
	private ArrayList<NodeProto> replicas;
	private NodeProto shardLeader ;
	private PaxosDetailsMsgType type;
	public PaxosDetailsMsg(NodeProto source, String shardId , PaxosDetailsMsgType type)
	{
		this.source = source;
		this.type = type;
		this.shardID = shardId;
	}
	
	public NodeProto getSource()
	{
		return this.source;
	}
	
	public PaxosDetailsMsgType getMsgType()
	{
		return this.type;
	}
	
	public String getShardId()
	{
		return this.shardID;
	}
	
	public void setShardLeader(NodeProto leader)
	{
		 this.shardLeader = leader;
	}
	
	public void setReplicas(ArrayList<NodeProto> replicas){
		this.replicas = replicas;
	}
	
	public NodeProto getShardLeader()
	{
		return this.shardLeader;
	}
	
	public ArrayList<NodeProto> getReplicas()
	{
		return this.replicas;
	}
	
	@Override
	public String toString() {
		StringBuffer bf = new StringBuffer();
		bf.append("Source - "+source.getHost()+":"+this.source.getPort());
		if(shardID != null)
			bf.append("Shard ID - "+this.shardID);
		if(shardLeader != null)
		bf.append("Shard Leader - "+shardLeader);
		if(replicas != null){
		bf.append("Replicas - \n");
		for(NodeProto node: replicas)
			bf.append("   "+node.getHost()+":"+node.getPort()+"\n");
		}
		
		return bf.toString();
	}

	

}
