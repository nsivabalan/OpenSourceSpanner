package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Level;

import spanner.node.PaxosLeader.TransactionStatus;

import spanner.message.BcastMsg;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;

import org.zeromq.ZMQ;

import spanner.common.Common.TwoPCMsgType;

import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.State;
import spanner.common.Resource;
import spanner.common.ResourceHM;

import spanner.common.MessageWrapper;

import spanner.common.Common.PaxosLeaderState;

import spanner.common.Common;

public class PaxosLeader extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket publisher = null;
	ZMQ.Socket socket = null;
	String leaderId = null;
	ArrayList<UUID> pendingTransactions = null;

	final class TransactionStatus {

		PaxosLeaderState state;
		Common.TransactionType transState;
		HashMap<String, ArrayList<String>> readSet;
		HashMap<String, HashMap<String, String>> writeSet;
		HashMap<String, HashMap<String, String>> data;
		String clientRoutingKey;
		String twoPCIdentifier;
		Boolean isCommitAckReceived;
		Boolean respondToClient;
		Set<String> acceptorListPrepare;
		Set<String> acceptorListCommit;
		Set<String> acceptorListAbort;

		public TransactionStatus(HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
		{			
			this.state = PaxosLeaderState.PREPARE;
			this.acceptorListPrepare = new HashSet<String>();	
			this.acceptorListCommit =  new HashSet<String>();
			this.acceptorListAbort =  new HashSet<String>();
			this.writeSet = writeSet;
			this.readSet = readSet;
			this.isCommitAckReceived = false;
		}		
	}


	private Map<UUID, TransactionStatus> uidTransactionStatusMap;
	private static ResourceHM localResource = null;
	public PaxosLeader(String shard, String shardid) throws IOException
	{
		super(shardid);
		this.leaderId = shard;
		context = ZMQ.context(1);

		ZMQ.Context context = ZMQ.context(1);

		String shardDetails = Common.getProperty(shard);
		System.out.println("Shard details "+shardDetails);
		String[] splits = shardDetails.split(";");
		String hostport = Common.getProperty(splits[0]);
		System.out.println("Host port "+hostport);
		String[] hostdetails = hostport.split(":");
		String pubPort = Common.getProperty(shard+"Leader");
		publisher = context.socket(ZMQ.PUB);
		publisher.bind("tcp://*:"+Integer.parseInt(pubPort));

		// Socket to receive messages on
		System.out.println("Receiving message "+Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket = context.socket(ZMQ.PULL); 
		//socket.connect(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		
		//socket.bind(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket.bind("tcp://127.0.0.1:"+hostdetails[1]);
		pendingTransactions = new ArrayList<UUID>();

		this.uidTransactionStatusMap = new LinkedHashMap<UUID, TransactionStatus>();
		localResource = new ResourceHM(this.LOGGER);

	}





	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			System.out.println("Received Messsage "+receivedMsg);
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);
			System.out.println("Msg wrap updated ");
			if (msgwrap != null ) {
					
				try {
					if(msgwrap.getmessageclass() == ClientOpMsg.class )
					{
						ClientOpMsg msg = (ClientOpMsg) msgwrap.getDeSerializedInnerMessage();

						//Print msg
						System.out.println("Received " + msg);
						this.AddLogEntry(new String("Received "+msg), Level.INFO);

						//ProcessClientMessage(msg);
						System.out.println("TODO ");
					}
				
				if(msgwrap.getmessageclass() == BcastMsg.class )
				{
					BcastMsg msg = (BcastMsg) msgwrap.getDeSerializedInnerMessage();
					//Print msg
					System.out.println("Received " + msg);
					this.AddLogEntry(new String("Received "+msg), Level.INFO);
					//ProcessBcastMessage(msg);
					System.out.println("TODO ");
				}
				if(msgwrap.getmessageclass() == PaxosMsg.class )
				{
					PaxosMsg msg = (PaxosMsg) msgwrap.getDeSerializedInnerMessage();
					//Print msg
					System.out.println("Received " + msg);
					this.AddLogEntry(new String("Received "+msg), Level.INFO);

					TransactionStatus temp = uidTransactionStatusMap.get(msg.getUID());
					System.out.println("Trans state "+temp.state +" ------------------------ ");
					if (temp.state == PaxosLeaderState.PREPARE)
						//ProcessPrepareAck(msg.getUID(), msg.getNodeid());
						System.out.println("TODO ");

					else if (temp.state == PaxosLeaderState.COMMIT)
						//ProcessCommitAck(msg.getUID(), msg.getNodeid());
						System.out.println("TODO ");

					else if (temp.state == PaxosLeaderState.ABORT)
						//ProcessAbortAck(msg.getUID(), msg.getNodeid());
						System.out.println("TODO ");
				}

				if(msgwrap.getmessageclass() == TwoPCMsg.class )
				{
					TwoPCMsg msg = (TwoPCMsg) msgwrap.getDeSerializedInnerMessage();
					//Print msg
					System.out.println("Received " + msg);
					this.AddLogEntry(new String("Received "+msg), Level.INFO);
					if(msg.getMsgType() == TwoPCMsgType.INFO)
					{
						//ProcessInfoMessage(msg);
					}
					else if(msg.getMsgType() == TwoPCMsgType.COMMIT){
						//ProcessCommitMessage(msg);
					}
					else if(msg.getMsgType() == TwoPCMsgType.ABORT){
						//ProcessAbortMessage(msg);
					}

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

			}

		}
		this.close();
		socket.close();
		context.term();
	}

	/*private void ProcessInfoMessage(TwoPCMsg message) throws IOException
	{
		TransactionStatus transStatus = null;
		if(!uidTransactionStatusMap.containsKey(message.getUID()))
			transStatus = new TransactionStatus(null, message.getData());
		transStatus.transState = Common.TransactionType.READDONE;
		
		uidTransactionStatusMap.put(message.getUID(), transStatus);
		PaxosMsg paxosMsg = new PaxosMsg(this.nodeId, Common.PaxosMsgType.ACCEPT, message.getUID(), message.getData());
		SendPaxosMsg(paxosMsg);

	}

	private void ProcessCommitMessage(TwoPCMsg message)
	{

	}

	private void ProcessAbortMessage(TwoPCMsg message)
	{

	}

	private void ProcessBcastMessage(BcastMsg message)
	{
		System.out.println("Paxos Leader should not receive any Bcast msg");	
	}

	private void ProcessTwoPCMessage(TwoPCMsg message)
	{
		//TODO: yet to code
		System.out.println("Bcast msg // to be filled in later");
	}

	private void ProcessClientMessage(ClientOpMsg message)
	{
		try {
			if(message.getType() == Common.ClientOPMsgType.WRITE)
				this.ProcessWriteRequest(message.getUid(), message.getWriteSet(), message.getNodeid());
			else
				this.ProcessReadRequest(message.getUid(), message.getreadSet(), message.getNodeid());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	//Process New Append Request from Client
	public void ProcessWriteRequest(UUID uid, HashMap<String, HashMap<String,String>> writeSet, String clientRoutingKey) throws IOException 
	{
		System.out.println("Processing Write Request ******************  ");
		TransactionStatus temp = new TransactionStatus(null, writeSet);
		temp.clientRoutingKey = clientRoutingKey;
		temp.data = writeSet;
		temp.writeSet = writeSet;
		temp.respondToClient = true;
		temp.acceptorListPrepare.add(this.nodeId);
		this.uidTransactionStatusMap.put(uid, temp);
		
		PaxosMsg paxosMsg = new PaxosMsg(this.nodeId, Common.PaxosMsgType.ACCEPT, uid, writeSet);
		SendPaxosMsg(paxosMsg);
	}


	//Process New Read Request from Client
	public void ProcessReadRequest(UUID uid, HashMap<String, ArrayList<String>> readSet, String clientRequestKey) throws IOException
	{
	//	System.out.println("Processing Read Request !!!!!!!!!!!!!!!!!!!!! ");
		TransactionStatus temp = new TransactionStatus(readSet, null);
		temp.clientRoutingKey = clientRequestKey;
		temp.respondToClient = true;
		
		HashMap<String,HashMap<String, String>> readData = this.localResource.ReadResource(readSet);
		temp.data = readData;
		temp.isCommitAckReceived = true;
		uidTransactionStatusMap.put(uid, temp);
		ClientOpMsg read_msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.READ_RESPONSE, readData, uid);
		SendClientMessage(read_msg);
		System.out.println("Sent Read Data for UID - "+uid);		
		LOGGER.log(Level.FINE, new String("Sent Read Data for UID - "+uid));
	}


	//Process Ack from Acceptor
	public void ProcessCommitAck(UUID uid, String nodeAddress) throws IOException
	{
		System.out.println("Processing Commit Ack ");
		TransactionStatus temp = this.uidTransactionStatusMap.get(uid);
		temp.acceptorListCommit.add(nodeAddress);

		System.out.println("UID - "+uid);
		System.out.println("Acceptor List for Commit " + temp.acceptorListCommit.toString());
		StringBuilder sb=new StringBuilder();
		sb.append("UID - "+uid);
		sb.append("\nAcceptor List " + temp.acceptorListCommit.toString());
		this.AddLogEntry(sb.toString(), Level.INFO);

		if (temp.acceptorListCommit.size() >= Common.GetQuorumSize()) 
		{
			temp.state = Common.PaxosLeaderState.COMMIT;

			//TwoPCMsg msg = new TwoPCMsg(this.nodeId, TwoPCMsgType.INFO, uid);
			//msg.setClientRoutingKey(temp.clientRoutingKey);
			//	this.SendTPCMsg(msg);
			//temp.data =  this.localResource.WriteResource(temp.writeSet);
			//temp.isCommitAckReceived = true;
			System.out.println("Received Commit Ack from majority of acceptors ");
			if(temp.transState == Common.TransactionType.COMMIT){
				temp.state = Common.PaxosLeaderState.COMMIT_ACK;
				TwoPCMsg msg = new TwoPCMsg(uid, TwoPCMsgType.COMMIT);
				SendMessageToTPC(msg, temp.twoPCIdentifier);
			}
			ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.WRITE_RESPONSE, temp.data, uid);
			SendClientMessage(msg);
		}
		else
		{
			//Add if required.
		}
		this.uidTransactionStatusMap.put(uid, temp);

	}

	//Process Ack from Acceptor
	public void ProcessAbortAck(UUID uid, String nodeAddress) throws IOException
	{
		TransactionStatus temp = this.uidTransactionStatusMap.get(uid);
		temp.acceptorListAbort.add(nodeAddress);

		System.out.println("UID - "+uid);
		System.out.println("Acceptor List for Abort " + temp.acceptorListAbort.toString());
		StringBuilder sb=new StringBuilder();
		sb.append("UID - "+uid);
		sb.append("\nAcceptor List " + temp.acceptorListAbort.toString());
		this.AddLogEntry(sb.toString(), Level.INFO);

		if (temp.acceptorListAbort.size() >= Common.GetQuorumSize()) 
		{
			temp.state = Common.PaxosLeaderState.ABORT;

			//TwoPCMsg msg = new TwoPCMsg(this.nodeId, TwoPCMsgType.INFO, uid);
			//msg.setClientRoutingKey(temp.clientRoutingKey);
			//	this.SendTPCMsg(msg);
			//temp.data =  this.localResource.WriteResource(temp.writeSet);
			//temp.isCommitAckReceived = true;
			if(temp.transState == Common.TransactionType.ABORT){
				temp.state = Common.PaxosLeaderState.ABORT_ACK;
				TwoPCMsg msg = new TwoPCMsg(uid, TwoPCMsgType.ABORT);
				SendMessageToTPC(msg, temp.twoPCIdentifier);
			}
			ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.WRITE_RESPONSE, temp.data, uid);
			SendClientMessage(msg);
		}
		else
		{
			//Add if required.
		}
		this.uidTransactionStatusMap.put(uid, temp);

	}

	//Process Ack from Acceptor
	public void ProcessPrepareAck(UUID uid, String nodeAddress) throws Exception
	{
		TransactionStatus temp = this.uidTransactionStatusMap.get(uid);
		temp.acceptorListPrepare.add(nodeAddress);

		System.out.println("UID - "+uid);
		System.out.println("Acceptor List " + temp.acceptorListPrepare.toString());
		StringBuilder sb=new StringBuilder();
		sb.append("UID - "+uid);
		sb.append("\nAcceptor List " + temp.acceptorListPrepare.toString());
		this.AddLogEntry(sb.toString(), Level.INFO);
		
		if (temp.acceptorListPrepare.size() >= Common.GetQuorumSize()) 
		{
			temp.state = Common.PaxosLeaderState.ACCEPT;
			System.out.println("Majority reached !!!!! ");
			//TwoPCMsg msg = new TwoPCMsg(this.nodeId, TwoPCMsgType.INFO, uid);
			//msg.setClientRoutingKey(temp.clientRoutingKey);
			//	this.SendTPCMsg(msg);
			//temp.data =  this.localResource.WriteResource(temp.writeSet);
			//temp.isCommitAckReceived = true;
			if(temp.respondToClient == true)
			{
				System.out.println("Sending commit wirte Set Command ");
				temp.state = PaxosLeaderState.COMMIT;
				temp.acceptorListCommit.add(this.nodeId);
				System.out.println("Writing writeset to localresource &&&&&&&&&&& "+temp.data);
				this.localResource.WriteResource(temp.data);
				PaxosMsg paxosMsg = new PaxosMsg(this.nodeId, Common.PaxosMsgType.COMMIT, uid, temp.data);
				SendPaxosMsg(paxosMsg);
				
				System.out.println("Client address "+temp.clientRoutingKey);
				ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.READ_RESPONSE, temp.data, uid);
				SendClientMessage(msg);
			}
			else if(temp.transState == Common.TransactionType.READDONE){
				System.out.println("Responding back to TPC");
				temp.state = Common.PaxosLeaderState.COMMIT;
				TwoPCMsg msg = new TwoPCMsg(uid, TwoPCMsgType.ACK);
				SendMessageToTPC(msg, temp.twoPCIdentifier);
			}
			else if(temp.transState == Common.TransactionType.COMMIT){
				System.out.println("Should not be reachable");
			}
			ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.WRITE_RESPONSE, temp.data, uid);
			SendClientMessage(msg);
		}
		else
		{
			//Add if required.
		}
		this.uidTransactionStatusMap.put(uid, temp);
	}

	private void SendMessageToTPC(TwoPCMsg message, String twoPCIdentifier)
	{
		System.out.println("Sent " + message);
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect(twoPCIdentifier);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );	
	}

	private void SendClientMessage(ClientOpMsg message)
	{
		//Print msg
		System.out.println("Sent " + message);
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect(message.getNodeid());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );

	}

	private void SendPrepareMessage(PaxosMsg message)
	{
		SendMessagetoAcceptors(message.toString().getBytes());
	}


	//Broadcast append request to all acceptors.
	public void SendPaxosMsg(PaxosMsg msg) throws IOException
	{		
		//Print msg
		System.out.println("Sent " + msg);
		this.AddLogEntry("Sent "+msg, Level.INFO);

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		this.SendMessagetoAcceptors(msgwrap.getSerializedMessage().getBytes());
	}


	//Add a new log entry.
	public void AddLogEntry(String message, Level level){		
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}

	private void SendMessagetoAcceptors(byte[] msg)
	{
		publisher.send(msg, 0);
	}*/

	public static void main(String[] args) throws IOException {
		if(args.length != 2)
			throw new IllegalArgumentException("Pass ShardID ReplicaID");

		PaxosLeader paxosLeader;
		try {
			paxosLeader = new PaxosLeader(args[0], args[1]);
			new Thread(paxosLeader).start();
		//	paxosLeader.listenToMsg();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
