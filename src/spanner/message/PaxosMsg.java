package spanner.message;

import java.util.HashMap;
import java.util.UUID;

import spanner.message.MessageBase;
import spanner.node.BallotNumber;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;

import spanner.common.Common;
import spanner.common.Common.PaxosMsgType;

/*
 * This class is used to model the messages dealing with paxos, like the accept msg, acknowledge msg and commit or abort msg which 
 * the paxos leader sends to all the acceptors in its paxos group
 */

public class PaxosMsg extends MessageBase{
	private String uidstr;
	private NodeProto source;
	private PaxosMsgType type;
	private BallotNumber ballotNo;
	private BallotNumber acceptNo;
	private ElementsSetProto acceptValue;
	private int logPositionNumber;
	//Prepare message
/*	public PaxosMsg(NodeProto source, PaxosMsgType type)
	{
		this.nodeAddress = source;
		this.type = type;
	}*/
	
	public int getLogPositionNumber() {
		return logPositionNumber;
	}

	public void setLogPositionNumber(int logPositionNumber) {
		this.logPositionNumber = logPositionNumber;
	}

	public PaxosMsg(NodeProto source, String uid, PaxosMsgType type, BallotNumber ballotNo)
	{
		this.uidstr = uid;
		this.source = source;
		this.type = type;
		this.ballotNo = ballotNo;
	}
	
	public PaxosMsg(NodeProto source,String uid, PaxosMsgType type, BallotNumber ballotNo, BallotNumber acceptNo, ElementsSetProto acceptVal)
	{
		this.source = source;
		this.type = type;
		this.ballotNo = ballotNo;
		this.acceptNo = acceptNo;
		this.acceptValue = acceptVal;
		this.uidstr = uid;
	}
	
	public PaxosMsg(NodeProto source,String uid, PaxosMsgType type, BallotNumber ballotNo, ElementsSetProto acceptVal)
	{
		this.source = source;
		this.type = type;
		this.ballotNo = ballotNo;
		this.acceptValue = acceptVal;
		this.uidstr = uid;
	}
	
	public PaxosMsg(NodeProto source,String uid, PaxosMsgType type,  ElementsSetProto acceptVal)
	{
		this.source = source;
		this.type = type;
		this.acceptValue = acceptVal;
		this.uidstr = uid;
	}
	
	public BallotNumber getBallotNumber()
	{
		return this.ballotNo;
	}
	
	public BallotNumber getAcceptNo()
	{
		return this.acceptNo;
	}
	
	
	public String getUID()
	{
		return this.uidstr;
	}
	
	public ElementsSetProto getAcceptValue()
	{
		return this.acceptValue;
	}
	
	public Common.PaxosMsgType getType() {
		return type;
	}
	
	public void setType(Common.PaxosMsgType type) {
		this.type = type;
	}

	public NodeProto getSource() {
		return source;
	}

	public void setSource(NodeProto nodeid) {
		this.source = nodeid;
	}



	@Override
	public String toString() {
		StringBuilder bf = new StringBuilder();
		
		bf.append(this.getClass().getName() + " - " + this.type);
		bf.append("\nUID - "+this.uidstr);
		bf.append("\n Source - " + this.source);
		bf.append("\n MsgType - " + this.type);
		if(ballotNo != null)
			bf.append("\n Ballot Number - "+ballotNo);
		if(acceptNo != null)
			bf.append("\b Accept Number - "+acceptNo);
		if(acceptValue != null)
			bf.append("\b Accept Value - "+acceptValue);
		bf.append("\n");
		
		return bf.toString();
	}
	
	
	
}