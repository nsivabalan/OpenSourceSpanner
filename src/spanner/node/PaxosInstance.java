package spanner.node;

import java.util.HashSet;

import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;

public class PaxosInstance{
	BallotNumber ballotNo;
	String uid;
	BallotNumber acceptedNumber;
	ElementsSetProto acceptedValue;
	HashSet<NodeProto> acks;
	HashSet<NodeProto> accepts;
	HashSet<NodeProto> decides;
	Long startTime;
	boolean isDecideComplete;
	BallotNumber highest;
	boolean isAcceptSent ;
	boolean isDecideSent ;
	boolean isCommited ;
	
	public PaxosInstance(String uid, BallotNumber ballotNo)
	{
		this.ballotNo = ballotNo;
		this.uid = uid;
		this.acks = new HashSet<NodeProto>();
		this.highest = null;
		this.accepts = new HashSet<NodeProto>();
		this.decides = new HashSet<NodeProto>();
	}
	
	public PaxosInstance(String uid, BallotNumber ballotNo, ElementsSetProto acceptedValue)
	{
		this.ballotNo = ballotNo;
		this.uid = uid;
		this.acks = new HashSet<NodeProto>();
		this.highest = null;
		this.accepts = new HashSet<NodeProto>();
		this.acceptedValue = acceptedValue;
		this.decides = new HashSet<NodeProto>();
	}
	
	
	public PaxosInstance(String uid2, BallotNumber ballotNumber,
			BallotNumber acceptNo, ElementsSetProto acceptValue) {
			this.uid = uid2;
			this.ballotNo = ballotNumber;
			this.acceptedNumber = acceptNo;
			this.acceptedValue = acceptValue;
			this.acks = new HashSet<NodeProto>();
			this.highest = null;
			this.accepts = new HashSet<NodeProto>();
			this.decides = new HashSet<NodeProto>();
	}
	
	public void setUID(String uid)
	{
		this.uid = uid;
	}
	
	public String getUID()
	{
		return this.uid;
	}
	
	public boolean isAcceptSent()
	{
		return this.isAcceptSent;
	}
	
	public void setAcceptSent()
	{
		this.isAcceptSent = true;
	}
	
	public BallotNumber getBallotNumber()
	{
		return this.ballotNo;
	}
	
	public int getAckCount()
	{
		return this.acks.size();
	}
	
	public void setTimeStamp(long timeStamp)
	{
		this.startTime = timeStamp;
	}
	
	public long getTimeStamp()
	{
		return this.startTime;
	}
	
	public HashSet<NodeProto> getAckList()
	{
		return this.acks;
	}
	
	public void addToDecideList(NodeProto node)
	{
		this.decides.add(node);
	}
	
	public int getDecidesCount()
	{
		return this.decides.size();
	}
	
	public void addAcceptorToAck(NodeProto node)
	{
		this.acks.add(node);
	}
	
	public void setBallotNumber(BallotNumber ballotNo)
	{
		this.ballotNo = ballotNo;
	}
	public void setAcceptNumber(BallotNumber acceptNo)
	{
		this.acceptedNumber = acceptNo;
	}
	
	public boolean isCommited()
	{
		return this.isCommited;
	}
	
	public void setCommited()
	{
		this.isCommited = true;
	}
	
	public void addtoAcceptList(NodeProto node)
	{
		this.accepts.add(node);
	}
	
	public int getAcceptCount()
	{
		return this.accepts.size();
	}
	
	public int getAcceptCountExcldn(NodeProto node)
	{
		if(accepts.contains(node)) return accepts.size()-1;
		else return accepts.size();
			
	}
	
	public HashSet<NodeProto> getAcceptList()
	{
		return this.accepts;
	}
	
	public void setAcceptedValue(ElementsSetProto value)
	{
		this.acceptedValue = value;
	}
	
	public BallotNumber getHighestAcceptNo()
	{
		return this.highest;
	}
	
	public void setHighestAcceptNo(BallotNumber ballotNo)
	{
		this.highest = ballotNo;
	}
	
	public BallotNumber getAcceptedNumber()
	{
		return this.acceptedNumber;
	}
	
	public ElementsSetProto getAcceptedValue()
	{
		return this.acceptedValue;
	}
	
	public String toString()
	{
		StringBuffer bf = new StringBuffer();
		bf.append("UID :: "+uid);
		bf.append("Ballot No ::"+ballotNo);
		bf.append("Accpeted Number :: "+acceptedNumber);
		bf.append("Accepted Value :: "+acceptedValue);
		return bf.toString();
	}
	
}