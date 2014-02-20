package spanner.message;

import java.util.ArrayList;
import java.util.HashMap;

import spanner.common.Common.MetaDataMsgType;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionMetaDataProto;
import spanner.protos.Protos.TransactionProto;

public class MetaDataMsg extends MessageBase{

	private NodeProto source = null;
	private String transID = null;
	private HashMap<String, ArrayList<String>> readSet;
	private HashMap<String, HashMap<String, String>> writeSet;
	private TransactionMetaDataProto transaction ;
	private MetaDataMsgType type;
	
	public MetaDataMsg(NodeProto source, HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet, MetaDataMsgType type)
	{
		this.source = source;
		this.readSet = readSet;
		this.writeSet = writeSet;
		this.type = type;
	}
	
	public MetaDataMsg(NodeProto source, HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet, MetaDataMsgType type, String uid)
	{
		this.source = source;
		this.readSet = readSet;
		this.writeSet = writeSet;
		this.type = type;
		this.transID = uid;
	}
	
	public MetaDataMsg(NodeProto source, TransactionMetaDataProto trans, MetaDataMsgType type)
	{
		this.source = source;
		this.transaction = trans;
		this.type = type;
	}
	
	public MetaDataMsgType getMsgType()
	{
		return this.type;
	}
	
	public HashMap<String, ArrayList<String>> getReadSet()
	{
		return this.readSet;
	}
	
	public HashMap<String, HashMap<String, String>> getWriteSet()
	{
		return this.writeSet;
	}
	
	public NodeProto getSource()
	{
		return this.source;
	}
	
	public TransactionMetaDataProto getTransaction()
	{
		return this.transaction;
	}
	
	public String getUID()
	{
		return this.transID;
	}
	
	@Override
	public String toString() {
		StringBuffer bf = new StringBuffer();
		bf.append("Source - "+source.getHost()+":"+this.source.getPort()+"\n");
		if(uid != null)
			bf.append("UID - "+this.transID+"\n");
		if(readSet != null){
			bf.append("ReadSet :: \n");
			for(String str:  readSet.keySet())
			{
				bf.append("     "+str+","+ readSet.get(str)+"\n");
			}
		}
		if(writeSet != null){
			bf.append("WriteSet :: \n");
			for(String str:  writeSet.keySet())
			{
				bf.append("     "+str+","+ writeSet.get(str)+"\n");
			}
		}
		if(transaction != null)
			bf.append("Transaction - "+transaction);
		
		return bf.toString();
	}

	

}
