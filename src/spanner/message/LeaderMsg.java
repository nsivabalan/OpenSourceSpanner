package spanner.message;

import spanner.common.Common.LeaderMsgType;
import spanner.protos.Protos.NodeProto;

public class LeaderMsg extends MessageBase{
	
	NodeProto source ;
	String shard;
	LeaderMsgType type;
	boolean isLeader ;
	
	public LeaderMsg(NodeProto source, LeaderMsgType type, String shard)
	{
		this.source = source;
		this.type = type;
		this.shard = shard;
	}
	
	public LeaderMsg(NodeProto source, LeaderMsgType type, boolean isLeader, String shard)
	{
		this.source = source;
		this.type = type;
		this.isLeader = isLeader;
		this.shard = shard;
	}
	
	public LeaderMsgType getType()
	{
		return this.type;
	}
	
	public String getShard()
	{
		return this.shard;
	}
	
	public boolean isLeader()
	{
		return this.isLeader;
	}
	
	public void setLeader()
	{
		this.isLeader = true;
	}
	
	public NodeProto getSource()
	{
		return this.source;
	}
	
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(source.getHost()+":"+source.getPort()+"\n");
		buffer.append(type+"\n");
		if(type == LeaderMsgType.RESPONSE)
			buffer.append(isLeader+"\n");
		return buffer.toString();
	}
	
}
