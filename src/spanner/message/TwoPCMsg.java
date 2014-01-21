package spanner.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import spanner.message.MessageBase;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionProto;

import spanner.common.Common;
import spanner.common.Common.TwoPCMsgType;

/*
 * This class is used to model the messages relating to interaction with 2PC coordinator
 */
public class TwoPCMsg extends MessageBase{
	private NodeProto source;
	private TransactionProto trans;
	private TwoPCMsgType type;
	private boolean toTPC;
	
	public TwoPCMsg(NodeProto source, TransactionProto trans, TwoPCMsgType type )
	{
		this.source = source;
		this.trans = trans;
		this.type = type;
	}
	
	public TwoPCMsg(NodeProto source, TransactionProto trans, TwoPCMsgType type, boolean toTPC )
	{
		this.source = source;
		this.trans = trans;
		this.type = type;
		this.toTPC = toTPC;
	}
	
	public boolean isTwoPC()
	{
		return this.toTPC;
	}
	
	public NodeProto getSource()
	{
		return this.source;
	}
	
	public TransactionProto getTransaction()
	{
		return this.trans;
	}
	
	public TwoPCMsgType getMsgType()
	{
		return this.type;
	}
	
	@Override
	public String toString() {
		StringBuilder bf = new StringBuilder();
		
		bf.append("\n"+this.getClass().getName() + " - " + this.type);
		bf.append("\n Source - " + this.source);
		bf.append("\n UID - " + this.trans.getTransactionID());
		bf.append("\n readSet - " + this.trans.getReadSet());
		bf.append("\n writeSet - " + this.trans.getWriteSet());
		bf.append("\n");
		
		return bf.toString();
	}
	
	

}