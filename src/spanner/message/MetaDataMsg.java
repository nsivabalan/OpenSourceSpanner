package spanner.message;

import java.util.ArrayList;
import java.util.HashMap;

import spanner.common.Common.MetaDataMsgType;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.PartitionServerElementProto;
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
		bf.append("\n============================================================== \n" +
				"Source - "+source.getHost()+":"+this.source.getPort()+", ");
		if(uid != null)
			bf.append("UID - "+this.transID);
		bf.append("\n");
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
		if(transaction != null){
			bf.append("Transaction - \n");
			if(transaction.hasTransactionID())
				bf.append(" Tnx ID : "+transaction.getTransactionID()+"\n");
			if(transaction.hasHostNodeID())
				bf.append(" HostID : "+transaction.getHostNodeID()+"\n");
			if(transaction.hasTransactionStatus())
				bf.append(" Transaction Status : "+transaction.getTransactionStatus()+"\n");
			if(transaction.getReadSet() != null){
				if(transaction.getReadSet().getElementsCount() > 0){
				bf.append(" ReadSet :: \n");
				ElementsSetProto acceptValue = transaction.getReadSet();
				for(ElementProto elementProto:  acceptValue.getElementsList())
				{
					String temp = elementProto.getRow()+":";
					for(ColElementProto colElemProto: elementProto.getColsList())
					{
						temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
					}
					bf.append("   "+temp+"\n");
				}
			}
			}
			if(transaction.getWriteSet() != null){
				if(transaction.getWriteSet().getElementsCount() >0 ){
				bf.append(" WriteSet :: \n");
				ElementsSetProto acceptValue = transaction.getWriteSet();
				for(ElementProto elementProto:  acceptValue.getElementsList())
				{
					String temp = elementProto.getRow()+":";
					for(ColElementProto colElemProto: elementProto.getColsList())
					{
						temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
					}
					bf.append("   "+temp+"\n");
				}
			}
			}
			if(transaction.getReadSetServerToRecordMappings() != null)
			{
				if(transaction.getReadSetServerToRecordMappings().getPartitionServerElementCount() >0){
				bf.append(" ReadSet Server Mappings :\n");
				for(PartitionServerElementProto partitionServer : transaction.getReadSetServerToRecordMappings().getPartitionServerElementList())
				{
					NodeProto dest = partitionServer.getPartitionServer().getHost();
					bf.append("   Server "+dest.getHost()+":"+dest.getPort()+" = ");
					for(ElementProto elementProto:  partitionServer.getElements().getElementsList())
					{
						String temp = elementProto.getRow()+":";
						for(ColElementProto colElemProto: elementProto.getColsList())
						{
							temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
						}
						bf.append("      "+temp+"::");
					}
					bf.append("\n");
				}
				}
			}
			if(transaction.getWriteSetServerToRecordMappings() != null)
			{
				if(transaction.getWriteSetServerToRecordMappings().getPartitionServerElementCount() > 0){
				bf.append(" WriteSet Server Mappings :\n");
				for(PartitionServerElementProto partitionServer : transaction.getWriteSetServerToRecordMappings().getPartitionServerElementList())
				{
					NodeProto dest = partitionServer.getPartitionServer().getHost();
					bf.append("   Server "+dest.getHost()+":"+dest.getPort()+" = ");
					for(ElementProto elementProto:  partitionServer.getElements().getElementsList())
					{
						String temp = elementProto.getRow()+":";
						for(ColElementProto colElemProto: elementProto.getColsList())
						{
							temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
						}
						bf.append("      "+temp+"::");
					}
					bf.append("\n");
				}
				}
			}
			if(transaction.hasTwoPC())
				bf.append(" TwoPC : "+transaction.getTwoPC().getHost()+":"+transaction.getTwoPC().getPort());
		}
		bf.append("\n============================================================== \n");
		
		return bf.toString();
	}

	

}
