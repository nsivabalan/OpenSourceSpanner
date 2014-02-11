package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
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


import spanner.locks.LockTableOld;
import spanner.message.BcastMsg;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosDetailsMsg;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

import org.zeromq.ZMQ;

import spanner.common.Common.TwoPCMsgType;

import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.State;
import spanner.common.Common.TransactionType;
import spanner.common.Resource;
import spanner.common.ResourceHM;

import spanner.common.MessageWrapper;

import spanner.common.Common.PaxosLeaderState;

import spanner.common.Common;

public class Participant extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket publisher = null;
	ZMQ.Socket socket = null;
	NodeProto leaderId = null;
	ArrayList<UUID> pendingTransactions = null;
	NodeProto nodeAddress = null;
	ArrayList<NodeProto> acceptors;
	TwoPC twoPhaseCoordinator = null;
	LockTableOld lockTable = null;
	BallotNumber ballotNo;
	String shard;
	int acceptNo;
	int acceptValue;
	NodeProto metadataService ;
	boolean isLeader ;
	BufferedReader br = null;
	final class TransactionStatus {

		NodeProto twoPC;
		NodeProto source;
		TransactionProto trans;
		Boolean isCommitAckReceived;
		Set<String> acceptorListPrepare;
		Set<String> acceptorListCommit;
		Set<String> acceptorListAbort;

		public TransactionStatus(NodeProto source, TransactionProto trans)
		{			
			this.acceptorListPrepare = new HashSet<String>();	
			this.acceptorListCommit =  new HashSet<String>();
			this.acceptorListAbort =  new HashSet<String>();
			this.isCommitAckReceived = false;
			this.source = source;
			this.trans = trans;
		}		
	}


	private Map<UUID, TransactionStatus> uidTransactionStatusMap;
	private static ResourceHM localResource = null;
	public Participant(String shard, String shardid) throws IOException
	{
		super(shardid);
		this.shard = shard;
		context = ZMQ.context(1);

		ZMQ.Context context = ZMQ.context(1);

		String shardDetails = Common.getProperty(shard);
		System.out.println("Shard details "+shardDetails);
		String[] splits = shardDetails.split(";");
		String hostport = Common.getProperty(splits[0]);
		System.out.println("Host port "+hostport);
		String[] hostdetails = hostport.split(":");

		// Socket to receive messages on
		System.out.println("Receiving message "+Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket = context.socket(ZMQ.PULL); 
		//socket.connect(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));

		//socket.bind(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket.bind("tcp://127.0.0.1:"+hostdetails[1]);
		pendingTransactions = new ArrayList<UUID>();
		this.uidTransactionStatusMap = new LinkedHashMap<UUID, TransactionStatus>();
		localResource = new ResourceHM(this.LOGGER);
		lockTable = new LockTableOld();
		InetAddress addr = InetAddress.getLocalHost();
		nodeAddress = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(Integer.parseInt(hostdetails[1])).build();
		System.out.println("Participant node address ****** "+nodeAddress);
		twoPhaseCoordinator = new TwoPC(nodeAddress, context);
		ballotNo = new BallotNumber(-1, Integer.parseInt(shardid));
		acceptNo = -1;
		acceptValue = -1;
		acceptors = new ArrayList<NodeProto>();
		String[] mds = Common.getProperty("mds").split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		sendPaxosMsgRequestingAcceptors();
		br = new BufferedReader(new InputStreamReader(System.in));
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
					if(msgwrap.getmessageclass() == ClientOpMsg.class )
					{
						
						ClientOpMsg msg = (ClientOpMsg) msgwrap.getDeSerializedInnerMessage();

						//Print msg
						//System.out.println("Received " + msg);
						this.AddLogEntry(new String("Received "+msg), Level.INFO);
						if(msg.getMsgType() == ClientOPMsgType.READ){
						
							ProcessClientReadMessage(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.UNLOCK){
							ProcessUnLockMessage(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.RELEASE_RESOURCE)
						{
							ProcessClientReleaseResourceMessage(msg);
						}
						else{
						
							ProcessClientWriteMessage(msg);
						}
					}

					if(msgwrap.getmessageclass() == PaxosDetailsMsg.class )
					{
						PaxosDetailsMsg msg = (PaxosDetailsMsg)msgwrap.getDeSerializedInnerMessage();
						handlePaxosDetailsMsg(msg);
					}
					/*if(msgwrap.getmessageclass() == BcastMsg.class )
				{
					BcastMsg msg = (BcastMsg) msgwrap.getDeSerializedInnerMessage();
					//Print msg
					System.out.println("Received " + msg);
					this.AddLogEntry(new String("Received "+msg), Level.INFO);
					ProcessBcastMessage(msg);
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
						ProcessPrepareAck(msg.getUID(), msg.getNodeid());

					else if (temp.state == PaxosLeaderState.COMMIT)
						ProcessCommitAck(msg.getUID(), msg.getNodeid());

					else if (temp.state == PaxosLeaderState.ABORT)
						ProcessAbortAck(msg.getUID(), msg.getNodeid());
				}*/

					if(msgwrap.getmessageclass() == TwoPCMsg.class )
					{
						System.out.println("Two PC msg received ..... ");
						TwoPCMsg msg = (TwoPCMsg) msgwrap.getDeSerializedInnerMessage();
						//Print msg
						System.out.println("Received " + msg);
						this.AddLogEntry(new String("Received "+msg), Level.INFO);
						
						if(msg.getMsgType() == TwoPCMsgType.INFO)
						{
							//ProcessInfoMessage(msg);
							twoPhaseCoordinator.ProcessInfoMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.PREPARE)
						{
							//ProcessInfoMessage(msg);
							twoPhaseCoordinator.ProcessPrepareMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.COMMIT){
							if(msg.isTwoPC())
								twoPhaseCoordinator.ProcessCommitMessage(msg);
							else
								ProcessCommitMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.ABORT){
							if(msg.isTwoPC())
								twoPhaseCoordinator.ProcessAbortMessage(msg);
							else
								ProcessAbortMessage(msg);
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
				System.out.println("ELSE LOOP ??? ");
			}

		}
		this.close();
		socket.close();
		context.term();
	}

	
	public void listenToMessage() throws NumberFormatException, IOException
	{
		BufferedReader br1 = new BufferedReader(new InputStreamReader(System.in));
		int option = -1;
		String line = null;
		System.out.println("1. Print Lock Tables 2. Exit");
		line = br.readLine();
		while(line != null)
			line = br.readLine();
		option = Integer.parseInt(line);
		while(option != 2)
		{
			if(option == 1)
			{
				printLocks();
			}
			System.out.println("1. Print Lock Tables 2. Exit");
			line = br.readLine();
			while(line != null)
				line = br.readLine();
			option = Integer.parseInt(line);
		}
	}
	
	private void printLocks()
	{
		lockTable.printLocks();
	}
	
	private void ProcessUnLockMessage(ClientOpMsg msg) throws IOException
	{
		TransactionProto trans = msg.getTransaction();
		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			lockTable.releaseLock(element.getRow(), trans.getTransactionID());
		}
		System.out.println("Released all locks for trans "+msg.getTransaction().getTransactionID());
		printLocks();
		System.out.println("Press enter to send response to TwoPC ");
		br.readLine();
	}
	
	private void handlePaxosDetailsMsg(PaxosDetailsMsg msg)
	{
		ArrayList<NodeProto> acceptorList = msg.getReplicas();
		for(NodeProto node: acceptorList)
		{
			if(node != nodeAddress)
				acceptors.add(node);
		}
		System.out.println("List of Acceptors "+acceptors);
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
	
	private void ProcessCommitMessage(TwoPCMsg message) throws IOException
	{
		NodeProto twoPC = message.getSource();
		TransactionProto trans = message.getTransaction();
		
		//TransactionStatus temp = new TransactionStatus(twoPC, trans);
		boolean isWritten = localResource.WriteResource(trans.getWriteSet());
		if(isWritten){
			TransactionProto transaction = TransactionProto.newBuilder()
					.setTransactionID(trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ACTIVE)
					.setWriteSet(trans.getWriteSet())
					.build();

			System.out.println("Commited data ::::: ");
			printLocks();
			System.out.println("Releasing Resources ");
			for(ElementProto element : trans.getWriteSet().getElementsList())
			{
				lockTable.releaseLock(element.getRow(), trans.getTransactionID());
			}
			
			for(ElementProto element : trans.getReadSet().getElementsList())
			{
				lockTable.releaseLock(element.getRow(), trans.getTransactionID());
			}
			
			printLocks();
			System.out.println("Press enter to send response to TwoPC ");
			br.readLine();
			
			
			TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, transaction, TwoPCMsgType.COMMIT, true);

			SendTwoPCMessage(commit_response, twoPC);
			System.out.println("Sending Commit Ack for UID - "+trans.getTransactionID());		
			LOGGER.log(Level.FINE, new String("Sent Commit Ack for UID - "+trans.getTransactionID()));
		}
		else{
			System.out.println("Failed to commit data for "+trans.getTransactionID());		
			LOGGER.log(Level.FINE, new String("Failed to commit data for "+trans.getTransactionID()));	
		}
	}

	private void ProcessAbortMessage(TwoPCMsg message) throws IOException
	{
		NodeProto twoPC = message.getSource();
		TransactionProto trans = message.getTransaction();
		System.out.println("Lock tables");
		printLocks();
		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			lockTable.releaseLock(element.getRow(), trans.getTransactionID());
		}
		System.out.println("Released all resources. No ack sent");
		printLocks();
		
		System.out.println("Press enter to send response to TwoPC");
		br.readLine();
		TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT, true);

		SendTwoPCMessage(commit_response, twoPC);
		System.out.println("Sending Abort Ack for UID - "+trans.getTransactionID());		
		LOGGER.log(Level.FINE, new String("Sent Abort Ack for UID - "+trans.getTransactionID()));
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

	private void ProcessClientReleaseResourceMessage(ClientOpMsg message)
	{
		System.out.println("Inside process client release resource msg");
		TransactionProto trans = message.getTransaction();

		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			lockTable.releaseLock(element.getRow(), trans.getTransactionID());
		}
		System.out.println("Released all resources. No ack sent");
		
	}
	
	private void ProcessClientReadMessage(ClientOpMsg message) throws IOException
	{
		
		System.out.println("Inside process client read msg");
		NodeProto transClient = message.getSource();
		TransactionProto trans = message.getTransaction();

		TransactionStatus temp = new TransactionStatus(transClient, trans);
		boolean isReadLock = true;
		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			if(!lockTable.acquireLock(element.getRow(), trans.getTransactionID()))
					isReadLock = false;
		}
		
		System.out.println("Acquired all read locks.");
		printLocks();
		System.out.println("Press enter to print lock tables");

		br.readLine();
		
		ElementsSetProto readValues = localResource.ReadResource(trans.getReadSet());
		TransactionProto transaction = TransactionProto.newBuilder()
				.setTransactionID(trans.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(readValues)
				.build();
		System.out.println("Preparing Client Read Response");
		ClientOpMsg read_response = null;
		if(isReadLock)
			read_response = new ClientOpMsg(nodeAddress, transaction, ClientOPMsgType.READ_RESPONSE, true);
		else
			read_response = new ClientOpMsg(nodeAddress, transaction, ClientOPMsgType.READ_RESPONSE, false);

		SendClientMessage(read_response, transClient);
		System.out.println("Sent Read Data for UID - "+trans.getTransactionID());		
		LOGGER.log(Level.FINE, new String("Sent Read Data for UID - "+trans.getTransactionID()));

	}

	private void ProcessClientWriteMessage(ClientOpMsg message) throws IOException
	{
		NodeProto twoPC = message.getSource();
		TransactionProto trans = message.getTransaction();

		//TransactionStatus temp = new TransactionStatus(twoPC, trans);
		//boolean isWritten = localResource.WriteResource(trans.getWriteSet());

		boolean isWriteLock = true;
		for(ElementProto element : trans.getWriteSet().getElementsList())
		{
			if(!lockTable.acquireLock(element.getRow(), trans.getTransactionID()))
					isWriteLock = false;
		}
		
		
		TransactionProto transaction = TransactionProto.newBuilder()
				.setTransactionID(trans.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setWriteSet(trans.getWriteSet())
				.build();
		TwoPCMsg write_response = null;
		if(isWriteLock)
			write_response = new TwoPCMsg(nodeAddress, transaction, TwoPCMsgType.PREPARE, true);
		else
			write_response = new TwoPCMsg(nodeAddress, transaction, TwoPCMsgType.ABORT, true);
		
		System.out.println("Prepared write respond to TwoPC.");
		if(isWriteLock)
			System.out.println("All write locks acquiired");
		else
			System.out.println("Not able to acquire all write locks");
		
		System.out.println("Lock tables ");
		printLocks();
		System.out.println("Press enter to send message to TwoPC");
		br.readLine();
		
		
		SendTwoPCMessage(write_response, twoPC);
		System.out.println("Sending Prepare Ack for Write Data for UID - "+trans.getTransactionID()+" "+trans);		
		LOGGER.log(Level.FINE, new String("Sent Prepare Ack for Write Data for UID - "+trans.getTransactionID()));

	}

	private void ProcessClientMessage(ClientOpMsg message)
	{
		/*try {
			if(message.getType() == Common.ClientOPMsgType.WRITE)
				this.ProcessWriteRequest(message.getUid(), message.getWriteSet(), message.getNodeid());
			else
				this.ProcessReadRequest(message.getUid(), message.getreadSet(), message.getNodeid());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}


	//Process New Append Request from Client
	/*public void ProcessWriteRequest(UUID uid, HashMap<String, HashMap<String,String>> writeSet, String clientRoutingKey) throws IOException 
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

	public void ProcessReadRequest(String uid, TransactionProto trans, NodeProto transClient)
	{

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
	}*/


	//Process Ack from Acceptor
	/*public void ProcessCommitAck(UUID uid, String nodeAddress) throws IOException
	{
		System.out.println("Processing Commit Ack ");
		TransactionStatus temp = this.uidTransactionStatusMap.get(uid);
		temp.acceptorListCommit.add(nodeAddress);

		System.out.println("UID - "+uid);
		System.out.println("Acceptor List for Commit " + temp.acceptorListCommit.toString());
		StringBuilder sb = new StringBuilder();
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
				//TwoPCMsg msg = new TwoPCMsg(uid, TwoPCMsgType.COMMIT);
				//SendMessageToTPC(msg, temp.twoPCIdentifier);
			}
			//ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.WRITE_RESPONSE, temp.data, uid);
			//SendClientMessage(msg);
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
			//ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.WRITE_RESPONSE, temp.data, uid);
			//SendClientMessage(msg);
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
			//ClientOpMsg msg = new ClientOpMsg(temp.clientRoutingKey, ClientOPMsgType.WRITE_RESPONSE, temp.data, uid);
			//SendClientMessage(msg);
		}
		else
		{
			//Add if required.
		}
		this.uidTransactionStatusMap.put(uid, temp);
	}

	/*private void SendMessageToTPC(TwoPCMsg message, String twoPCIdentifier)
	{
		System.out.println("Sent " + message);
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect(twoPCIdentifier);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );	
	}*/

	private void SendTwoPCMessage(TwoPCMsg message, NodeProto dest)
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

	private void SendClientMessage(ClientOpMsg message, NodeProto dest)
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
	}

	public static void main(String[] args) throws IOException {
		if(args.length != 2)
			throw new IllegalArgumentException("Pass ShardID ReplicaID");

		Participant paxosLeader;
		try {
			paxosLeader = new Participant(args[0], args[1]);
			new Thread(paxosLeader).start();
			//paxosLeader.listenToMessage();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
