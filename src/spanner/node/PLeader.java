package spanner.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PLeaderState;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosLeaderState;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.TwoPCMsgType;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosDetailsMsg;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.NodeProto;

public class PLeader extends Node{

	ZMQ.Context context = null;
	ZMQ.Socket publisher = null;
	ZMQ.Socket socket = null;
	NodeProto nodeAddress = null;
	int myId;
	NodeProto metadataService ;
	boolean isLeader ;
	String shard ;
	PLeaderState state ;
	ArrayList<NodeProto> acceptors;
	BallotNumber ballotNo = null;
	private HashMap<String, PaxosInstance> uidPaxosInstanceMap = null;
	private boolean isValueDecided ;
	
	public PLeader(String shard, String nodeId) throws IOException
	{
		super(nodeId);
		this.shard = shard;
		context = ZMQ.context(1);

		ZMQ.Context context = ZMQ.context(1);

		String shardDetails = Common.getProperty(shard);
		System.out.println("Shard details "+shardDetails);
		String[] splits = shardDetails.split(";");
		String hostport = Common.getProperty(splits[0]);
		System.out.println("Host port "+hostport);
		String[] hostdetails = hostport.split(":");
		LOGGER = Logger.getLogger(nodeId);
		
		// Socket to receive messages on
		System.out.println("Receiving message "+Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket = context.socket(ZMQ.PULL); 
		//socket.connect(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));

		//socket.bind(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket.bind("tcp://127.0.0.1:"+hostdetails[1]);
		InetAddress addr = InetAddress.getLocalHost();
		nodeAddress = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(Integer.parseInt(hostdetails[1])).build();
		System.out.println("Participant node address ****** "+nodeAddress);
		String[] mds = Common.getProperty("mds").split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		state = PLeaderState.INIT;
		myId = Integer.parseInt(nodeId);
		ballotNo = new BallotNumber(-1, myId);
		isValueDecided = false;
		uidPaxosInstanceMap = new HashMap<String, PaxosInstance>();
		sendPaxosMsgRequestingAcceptors();
	}
	
	
	private class PaxosInstance{
		BallotNumber ballotNo;
		String uid;
		BallotNumber acceptedNumber;
		String acceptedValue;
		
		public PaxosInstance(String uid, BallotNumber ballotNo)
		{
			this.ballotNo = ballotNo;
			this.uid = uid;
		}
		
		public void setAcceptNumber(BallotNumber acceptNo)
		{
			this.acceptedNumber = acceptNo;
		}
		
		public void setAcceptedValue(String value)
		{
			this.acceptedValue = value;
		}
		
		public BallotNumber getAcceptedNumber()
		{
			return this.acceptedNumber;
		}
		
		public String getAcceptedValue()
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
	
	private void sendPaxosMsgRequestingAcceptors()
	{
		PaxosDetailsMsg msg = new PaxosDetailsMsg(nodeAddress, shard, PaxosDetailsMsgType.ACCEPTORS);
		sendMsgToMDS(metadataService ,msg);
	}
	
	private void sendMsgToMDS(NodeProto dest, PaxosDetailsMsg message)
	{
		//Print msg
				System.out.println("Sent " + message);
				this.AddLogEntry("Sent "+message, Level.INFO);

				ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
				pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
				MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
				pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
				pushSocket.close();
	}
	
	
	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
		//	System.out.println("Received Messsage &&&&&&&&&&&&&& "+receivedMsg);
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);
		//	System.out.println("Msg wrap updated -------------------------------- ");
			if (msgwrap != null ) {

				try {
					if(msgwrap.getmessageclass() == ClientOpMsg.class && this.state == PLeaderState.ACTIVE)
					{
						
						ClientOpMsg msg = (ClientOpMsg) msgwrap.getDeSerializedInnerMessage();

						
					}

					if(msgwrap.getmessageclass() == PaxosDetailsMsg.class )
					{
						PaxosDetailsMsg msg = (PaxosDetailsMsg)msgwrap.getDeSerializedInnerMessage();
						handlePaxosDetailsMsg(msg);
					}
				

					
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else{
				System.out.println("ELSE LOOP ??? ");
			}

		}
		this.close();
		socket.close();
		context.term();
	}

	private void handlePaxosDetailsMsg(PaxosDetailsMsg msg)
	{
		acceptors = new ArrayList<NodeProto>();
		ArrayList<NodeProto> acceptorList = msg.getReplicas();
		for(NodeProto node: acceptorList)
		{
			if(node != nodeAddress)
				acceptors.add(node);
		}
		System.out.println("List of Acceptors "+acceptors);
		initPaxosInstance();
	}
	
	private void initPaxosInstance()
	{
		sendPrepareMessage();
	}
	
	private void sendPrepareMessage()
	{
		ballotNo = new BallotNumber(ballotNo.getBallotNo()+1, myId);
		String uid = java.util.UUID.randomUUID().toString();
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo);
		uidPaxosInstanceMap.put(uid, paxInstance);
		PaxosMsg msg = new PaxosMsg(nodeAddress, PaxosMsgType.PREPARE, ballotNo);
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				sendPaxosMsg(node, msg);
			}
		}
		
	}
	
	private void sendPaxosMsg(NodeProto dest, PaxosMsg msg){
		System.out.println("Sent " + msg);
		this.AddLogEntry("Sent "+msg, Level.INFO);

		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();	
	}
	
	
	
	
}
