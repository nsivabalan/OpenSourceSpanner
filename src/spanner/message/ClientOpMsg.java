package spanner.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import spanner.message.MessageBase;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionProto;

import spanner.common.Common;
import spanner.common.Common.ClientOPMsgType;

public class ClientOpMsg extends MessageBase{
	private NodeProto source;
	private TransactionProto trans;
	private ClientOPMsgType msgType;
	private Boolean isReadLock ;
	
	public ClientOpMsg(NodeProto source, TransactionProto trans, ClientOPMsgType type)
	{
		this.source = source;
		this.trans = trans;
		this.msgType = type;
	}
	
	public ClientOpMsg(NodeProto source, TransactionProto trans, ClientOPMsgType type, boolean isReadLock)
	{
		this.source = source;
		this.trans = trans;
		this.msgType = type;
		this.isReadLock = isReadLock;
	}
	
	public String toString() {
		StringBuilder bf = new StringBuilder();
		bf.append(this.getClass().getName() + " - " + this.msgType);
		bf.append("\n Source - " + this.source.getHost()+" "+this.source.getPort());
		bf.append("\n UID - " + this.trans.getTransactionID());		
		bf.append("\n ReadSet - " + this.trans.getReadSet());
		bf.append("\n Is ReadLock Acquired - "+this.isReadLock);
		bf.append("\n WriteSet - "+ this.trans.getWriteSet());
		bf.append("\n");
		
		return bf.toString();
	}
	
	public boolean isReadLockSet()
	{
		return this.isReadLock;
	}
	
	public TransactionProto getTransaction()
	{
		return this.trans;
	}
	
	public NodeProto getSource()
	{
		return this.source;
	}
	
	public ClientOPMsgType getMsgType()
	{
		return this.msgType;
	}
}