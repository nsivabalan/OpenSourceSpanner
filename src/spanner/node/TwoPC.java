package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;


import spanner.message.BcastMsg;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.PartitionServerElementProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

import org.zeromq.ZMQ;

import spanner.common.Common.TwoPCMsgType;

import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.State;
import spanner.common.Common.TransactionType;
import spanner.common.Resource;
import spanner.common.ResourceHM;

import spanner.common.MessageWrapper;

import spanner.common.Common.PaxosLeaderState;

import spanner.common.Common;

public class TwoPC implements Runnable{

	ArrayList<String> pendingTransactions = null;
	NodeProto nodeAddress = null;
	ZMQ.Context context = null;
	ZMQ.Socket socketPush = null;
	BufferedReader br = null;
	HashMap<String, TransactionType> uidTransTypeMap = null;
	Set<String> pendingTrans = null;

	final class TransactionStatus {

		NodeProto twoPC;
		NodeProto source;
		TransactionType transState;
		TransactionProto trans;
		Boolean isCommitAckReceived;
		Boolean isAbortSent;
		Long initTimeStamp = null;
		Set<NodeProto> paritcipantListPrepare;
		Set<NodeProto> paritcipantListCommit;
		Set<NodeProto> paritcipantListAbort;

		public TransactionStatus(NodeProto source, TransactionProto trans)
		{			
			this.paritcipantListPrepare = new HashSet<NodeProto>();	
			this.paritcipantListCommit =  new HashSet<NodeProto>();
			this.paritcipantListAbort =  new HashSet<NodeProto>();
			this.isCommitAckReceived = false;
			this.isAbortSent = false;
			transState = TransactionType.WRITEINIT;
			this.source = source;
			this.trans = trans;
		}		
	}


	private Map<String, TransactionStatus> uidTransactionStatusMap;
	private static ResourceHM localResource = null;
	protected static Logger LOGGER = null;

	public TwoPC(NodeProto nodeAddress, ZMQ.Context context ) throws IOException
	{

		this.context = context;
		pendingTransactions = new ArrayList<String>();
		br = new BufferedReader(new InputStreamReader(System.in));
		this.uidTransactionStatusMap = new LinkedHashMap<String, TransactionStatus>();
		LOGGER =  Logger.getLogger("TwoPC");
		localResource = new ResourceHM(this.LOGGER);
		this.nodeAddress = nodeAddress;
		uidTransTypeMap = new HashMap<String, TransactionType>();
		pendingTrans = new HashSet<String>();

	}


	/*private void ProcessInfoMessage(TwoPCMsg message) throws IOException
	{
		TransactionStatus transStatus = null;
		TransactionProto trans = message.getTransaction();
		if(!uidTransactionStatusMap.containsKey(trans.getTransactionID())){
			transStatus = new TransactionStatus(message.getSource(), trans);
			transStatus.source = message.getSource();
		}
		else{
			System.out.println("Shouldn't reach here ");
		}

		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
		//PaxosMsg paxosMsg = new PaxosMsg(this.nodeId, Common.PaxosMsgType.ACCEPT, message.getUID(), message.getData());
		//SendPaxosMsg(paxosMsg);

	}*/

	public void run()
	{
		while(true){
			checkForPendingTrans();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private synchronized void checkForPendingTrans() 
	{

		Long curTime = new Date().getTime();
		Set<String> pendingTransTemp = pendingTrans;
		System.out.println("--------------------------- TwoPC start ----------------------------- ");
		for(String uid: pendingTransTemp)
		{
			if(uidTransTypeMap.get(uid) != TransactionType.ABORT)
			{
				TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
				if(curTime - transStatus.initTimeStamp > Common.TPC_TIMEOUT)
				{	
					System.out.println("Transaction timed out.");
					if(uidTransTypeMap.get(uid) != TransactionType.ABORT)
					{
						System.out.println("Aborting the trans due to time out");
						TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, transStatus.trans, TwoPCMsgType.ABORT);
						pendingTrans.remove(uid);
						uidTransTypeMap.put(uid, TransactionType.ABORT);
						transStatus.transState = TransactionType.ABORT;
						uidTransactionStatusMap.put(uid, transStatus);
						SendTwoPCMessage(commit_response, transStatus.source);
						System.out.println("Sending Abort msg to Trans Client- "+uid);		
						LOGGER.log(Level.FINE, new String("Sent Abort msg to Trans Client- "+uid));	

						for(PartitionServerElementProto partitionServer : transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
						{
							NodeProto dest = partitionServer.getPartitionServer().getHost();
							sendAbortInitMessage(nodeAddress, dest, transStatus.trans, partitionServer.getElements());
						}

					}

				}
			}
		}
		System.out.println("--------------------------- TwoPC end ----------------------------- ");
	}

	public void ProcessInfoMessage(TwoPCMsg message)
	{
		System.out.println("--------------------- TWO PC --------------------- start -------------------- ");
		NodeProto transClient= message.getSource();
		TransactionProto trans = message.getTransaction();
		System.out.println("Received INFO msg from client &&&&& "+transClient.getHost()+":"+transClient.getPort());
		TransactionStatus transStatus = new TransactionStatus(transClient, trans);
		transStatus.initTimeStamp = System.currentTimeMillis();
		System.out.println(" RS :: "+trans.getReadSet()+"\n WS ::"+trans.getWriteSet());
		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
		System.out.println("Waiting for prepare ack from participants ");
		uidTransTypeMap.put(trans.getTransactionID(), TransactionType.WRITEINIT);
		pendingTrans.add(trans.getTransactionID());
		System.out.println("--------------------- TWO PC --------------------- end -------------------- ");
	}

	public synchronized void ProcessPrepareMessage(TwoPCMsg message) throws IOException
	{
		System.out.println("--------------------- TWO PC --------------------- start -------------------- ");
		NodeProto transClient= message.getSource();
		TransactionProto trans = message.getTransaction();
		String uid = trans.getTransactionID();
		System.out.println("Received Prepare msg from participant &&&&& "+transClient.getHost()+":"+transClient.getPort());
		if(pendingTrans.contains(trans.getTransactionID()))
		{

			TransactionStatus transStatus = uidTransactionStatusMap.get(trans.getTransactionID());

			if(uidTransTypeMap.get(uid) != TransactionType.ABORT){
				transStatus.paritcipantListPrepare.add(transClient);
				if(transStatus.paritcipantListPrepare.size() == transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount())
				{
					System.out.println("Received Prepare msg from all participants &&&&&&&&&& ");
					for(PartitionServerElementProto partitionServer : transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
					{
						uidTransTypeMap.put(trans.getTransactionID(), TransactionType.COMMIT);
						transStatus.initTimeStamp = System.currentTimeMillis();
						transStatus.transState = TransactionType.COMMIT;
						uidTransactionStatusMap.put(uid, transStatus);
						NodeProto dest = partitionServer.getPartitionServer().getHost();
						sendCommitInitMessage(nodeAddress, dest, trans, partitionServer.getElements());
					}
				}

			}
			else{
				System.out.println("Already aborted. No action taken");
				/*if(pendingTrans.contains(trans.getTransactionID()))
					pendingTrans.remove(trans.getTransactionID());*/
			}


		}
		else
			System.out.println("Already decision taken on the transaction. So no action taken");
		System.out.println("--------------------- TWO PC --------------------- end -------------------- ");
	}

	private void sendCommitInitMessage(NodeProto source, NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.COMMITTED)
				.setWriteSet(elements)
				.build();

		TwoPCMsg msg = new TwoPCMsg(source, trans , TwoPCMsgType.COMMIT);
		SendCommitInitMessage(msg, dest);
	}

	private void sendAbortInitMessage(NodeProto source, NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ABORTED)
				.setWriteSet(elements)
				.build();


		TwoPCMsg msg = new TwoPCMsg(source, trans , TwoPCMsgType.ABORT);
		SendTwoPCMessage(msg, dest);
	}

	private void SendCommitInitMessage(TwoPCMsg message, NodeProto dest)
	{
		System.out.println("Sending Commit Init msg - "+message.getTransaction().getTransactionID());		
		LOGGER.log(Level.FINE, new String("Sent Commit Init msg to Participant- "+dest.getHost()+":"+dest.getPort()));
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	public synchronized void ProcessCommitMessage(TwoPCMsg message) throws IOException
	{
		System.out.println("--------------------- TWO PC --------------------- start -------------------- ");
		NodeProto participant = message.getSource();
		TransactionProto trans = message.getTransaction();

		TransactionStatus transStatus = uidTransactionStatusMap.get(trans.getTransactionID());
		String uid = trans.getTransactionID();
		if(uidTransTypeMap.get(uid) != TransactionType.ABORT){
			transStatus.paritcipantListCommit.add(participant);
			uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
			System.out.println("Expected Count "+transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount());
			System.out.println("Actual Count "+transStatus.paritcipantListCommit.size());
			System.out.println(" "+transStatus.paritcipantListCommit);

			if(transStatus.paritcipantListCommit.size() == transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount())
			{
				TransactionProto clientResponse = TransactionProto.newBuilder()
						.setTransactionID(transStatus.trans.getTransactionID())
						.setTransactionStatus(TransactionStatusProto.COMMITTED)
						.setReadSet(transStatus.trans.getReadSet())
						.setWriteSet(transStatus.trans.getWriteSet())
						.build();

				pendingTrans.remove(trans.getTransactionID());
				TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, clientResponse, TwoPCMsgType.COMMIT);

				SendTwoPCMessage(commit_response, transStatus.source);
				System.out.println("Sending Commit msg to Trans Client- "+trans.getTransactionID());		
				LOGGER.log(Level.FINE, new String("Sent Commit msg to Trans Client- "+trans.getTransactionID()));
			}
		}
		else{
			System.out.println("Already aborted. No action taken");
		}
		System.out.println("--------------------- TWO PC --------------------- end -------------------- ");
	}

	public synchronized void ProcessAbortMessage(TwoPCMsg message) throws IOException
	{
		System.out.println("--------------------- TWO PC --------------------- start -------------------- ");
		NodeProto participant = message.getSource();
		TransactionProto trans = message.getTransaction();
		System.out.println("Abort msg from "+participant);
		String uid = trans.getTransactionID();
		TransactionStatus transStatus = uidTransactionStatusMap.get(trans.getTransactionID());
		if(uidTransTypeMap.get(uid) != TransactionType.ABORT){
			System.out.println("Received abort message from one participant for first time ");
			transStatus.transState = TransactionType.ABORT;
			transStatus.paritcipantListAbort.add(participant);
			//	System.out.println("Sending abort messages to all participants");
			//br.readLine();
			//	TwoPCMsg commit_init = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.COMMIT);

			System.out.println("Received Abort from one participant, hence Aborting the trans >>>>>>>>>>>   ");
			uidTransTypeMap.put(uid,  TransactionType.ABORT);
			transStatus.transState = TransactionType.ABORT;
			uidTransactionStatusMap.put(uid, transStatus);
			TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
			if(pendingTrans.contains(uid))
				pendingTrans.remove(trans.getTransactionID());
			SendTwoPCMessage(commit_response, transStatus.source);
			System.out.println("Sending Abort msg to Trans Client- "+trans.getTransactionID());		
			LOGGER.log(Level.FINE, new String("Sent Abort msg to Trans Client- "+trans.getTransactionID()));	


			for(PartitionServerElementProto partitionServer : transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
			{
				NodeProto dest = partitionServer.getPartitionServer().getHost();
				System.out.println("Dest "+dest);
				if(!dest.equals(participant))
					sendAbortInitMessage(nodeAddress, dest, trans, partitionServer.getElements());
			}

		}
		else{
			transStatus.paritcipantListAbort.add(participant);
			System.out.println("Expected Count "+transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount());
			System.out.println("Actual Count "+transStatus.paritcipantListAbort.size());
			/*if(transStatus.paritcipantListAbort.size() == transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount())
			{
				System.out.println("Received abort messages from  all participants");
				//br.readLine();
				TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				pendingTrans.remove(trans.getTransactionID());
				SendTwoPCMessage(commit_response, transStatus.source);
				System.out.println("Sending Abort msg to Trans Client- "+trans.getTransactionID());		
				LOGGER.log(Level.FINE, new String("Sent Abort msg to Trans Client- "+trans.getTransactionID()));
			}
			else{
				System.out.println("Still waiting for aborts from few more participants >>>>>>>>>>>. ");
			}*/
		}
		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
		System.out.println("--------------------- TWO PC --------------------- end -------------------- ");
	}


	public void ProcessClientReadMessage(ClientOpMsg message)
	{
		System.out.println("--------------------- TWO PC --------------------- start -------------------- ");
		NodeProto transClient = message.getSource();
		TransactionProto trans = message.getTransaction();

		TransactionStatus temp = new TransactionStatus(transClient, trans);
		ElementsSetProto readValues = localResource.ReadResource(trans.getReadSet());

		TransactionProto transaction = TransactionProto.newBuilder()
				.setTransactionID(trans.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setWriteSet(readValues)
				.build();


		ClientOpMsg read_response = new ClientOpMsg(transClient, transaction, ClientOPMsgType.READ_RESPONSE);

		SendClientMessage(read_response, transClient);
		System.out.println("Sent Read Data for UID - "+trans.getTransactionID());		
		LOGGER.log(Level.FINE, new String("Sent Read Data for UID - "+trans.getTransactionID()));
		System.out.println("--------------------- TWO PC --------------------- end -------------------- ");
	}

	public void ProcessClientWriteMessage(ClientOpMsg message)
	{
		System.out.println("--------------------- TWO PC --------------------- start -------------------- ");
		NodeProto twoPC = message.getSource();
		TransactionProto trans = message.getTransaction();

		TransactionProto transaction = TransactionProto.newBuilder()
				.setTransactionID(trans.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setWriteSet(trans.getWriteSet())
				.build();

		TwoPCMsg write_response = new TwoPCMsg(twoPC, transaction, TwoPCMsgType.PREPARE, true);

		SendTwoPCMessage(write_response, twoPC);
		System.out.println("Sending Prepare Ack for Write Data for UID - "+trans.getTransactionID());		
		LOGGER.log(Level.FINE, new String("Sent Prepare Ack for Write Data for UID - "+trans.getTransactionID()));
		System.out.println("--------------------- TWO PC --------------------- end -------------------- ");
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
		System.out.println("Sending TwoPCMsg " + message+" to "+dest.getHost()+":"+dest.getPort());
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );

	}

	private void SendClientMessage(ClientOpMsg message, NodeProto dest)
	{
		//Print msg
		System.out.println("Sending ClientOpMsg " + message+" to "+dest.getHost()+":"+dest.getPort());
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );

	}

	/*private void SendPrepareMessage(PaxosMsg message)
	{
		SendMessagetoAcceptors(message.toString().getBytes());
	}*/


	//Broadcast append request to all acceptors.
	/*public void SendPaxosMsg(PaxosMsg msg) throws IOException
	{		
		//Print msg
		System.out.println("Sent " + msg);
		this.AddLogEntry("Sent "+msg, Level.INFO);

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		this.SendMessagetoAcceptors(msgwrap.getSerializedMessage().getBytes());
	}*/


	//Add a new log entry.
	public void AddLogEntry(String message, Level level){		
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}

	/*private void SendMessagetoAcceptors(byte[] msg)
	{
		publisher.send(msg, 0);
	}*/

	/*public static void main(String[] args) throws IOException {
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
	}*/

}
