package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.MetaDataMsgType;
import spanner.common.Common.PaxosLeaderState;
import spanner.common.Common.TransactionType;
import spanner.common.Common.TwoPCMsgType;
import spanner.common.MessageWrapper;
import spanner.common.Resource;
import spanner.common.Common.State;
import spanner.message.ClientOpMsg;
import spanner.message.MetaDataMsg;
import spanner.message.TwoPCMsg;
import spanner.metadataservice.MetaDS;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.PartitionServerElementProto;
import spanner.protos.Protos.PartitionServerProto;
import spanner.protos.Protos.TransactionMetaDataProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

public class TransClient extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;
	int port = -1;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	NodeProto metadataService ;
	NodeProto transClient;
	HashMap<String, NodeProto> clientMappings ;

	public TransClient(String clientID, int port, boolean isNew) throws IOException
	{
		super(clientID, isNew);
		context = ZMQ.context(1);
		String[] tsClient = Common.getProperty("mds").split(":");
		InetAddress addr = InetAddress.getLocalHost();
		transClient = NodeProto.newBuilder().setHost(tsClient[0]).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		AddLogEntry("Listening to messages @ "+transClient.getHost()+":"+port);
		socket.bind("tcp://*:"+port);
		this.port = port;
		String[] mds = Common.getProperty("mds").split(":");
		if(mds[0].equalsIgnoreCase("localhost"))
			metadataService = NodeProto.newBuilder().setHost("127.0.0.1").setPort(Integer.parseInt(mds[1])).build();
		else
			metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		clientMappings = new HashMap<String, NodeProto>();
	}

	final class TransactionStatus {

		HashMap<NodeProto, Boolean> readLocks;
		TransactionType state;
		int noOfReadLocks = 0;
		TransactionProto trans;
		boolean isReadLockAcquired ;
		NodeProto twoPC ;

		public TransactionStatus(TransactionProto trans)
		{			
			this.trans = trans;
			this.readLocks = new HashMap<NodeProto, Boolean>();
			this.state = TransactionType.STARTED;
			for(PartitionServerElementProto node: trans.getReadSetServerToRecordMappings().getPartitionServerElementList())
			{
				readLocks.put(node.getPartitionServer().getHost(), false);
			}
			this.isReadLockAcquired = false;
			twoPC = null;
		}		
	}

	private Map<String, TransactionStatus> uidTransactionStatusMap = new HashMap<String, TransactionStatus>();



	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);
			if (msgwrap != null)
			{
				try {
					if (msgwrap.getmessageclass() == MetaDataMsg.class)
					{
						MetaDataMsg message = (MetaDataMsg)msgwrap.getDeSerializedInnerMessage();
						if(message.getMsgType() == MetaDataMsgType.RESPONSE)
							handleMetaDataResponse(message);
						else if(message.getMsgType() == MetaDataMsgType.REQEUST)
							handleMetaDataRequest(message);
					}
					else if (msgwrap.getmessageclass() == ClientOpMsg.class)
					{
						ClientOpMsg message = (ClientOpMsg)msgwrap.getDeSerializedInnerMessage();
						if(message.getMsgType() == ClientOPMsgType.READ_RESPONSE)
						{
							handleReadResponse(message);
						}
						else{
							ProcessClientOpMessage(message);
						}
					}
					else if (msgwrap.getmessageclass() == TwoPCMsg.class)
					{
						TwoPCMsg message = (TwoPCMsg)msgwrap.getDeSerializedInnerMessage();
						handleTwoPCResponse(message);
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		socket.close();
		context.term();
	}

	/**
	 * Method used to process transaction request from user client
	 * @param msg
	 */
	private void handleMetaDataRequest(MetaDataMsg msg)
	{
		AddLogEntry("Handling Meta data request "+msg);
		String uid = java.util.UUID.randomUUID().toString();
		clientMappings.put(uid, msg.getSource());
		MetaDataMsg message = new MetaDataMsg(transClient, msg.getReadSet(), msg.getWriteSet(), MetaDataMsgType.REQEUST, uid);
		sendMetaDataMsg(message);
	}

	/**
	 * Method used to send Metadata msg to MDS
	 * @param msg
	 */
	private void sendMetaDataMsg(MetaDataMsg msg)
	{
		AddLogEntry("Sending Meta data request "+msg+" to MDService "+metadataService.getHost()+":"+metadataService.getPort());
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+metadataService.getHost()+":"+metadataService.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	/**
	 * Method used to handle meta data response from MDS
	 * @param msg
	 */
	private void handleMetaDataResponse(MetaDataMsg msg)
	{
		AddLogEntry("\nHandling meta data response from MDS :: "+msg);
		TransactionMetaDataProto transaction = msg.getTransaction();
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(transaction.getReadSet())
				.setWriteSet(transaction.getWriteSet())
				.setReadSetServerToRecordMappings(transaction.getReadSetServerToRecordMappings())
				.setWriteSetServerToRecordMappings(transaction.getWriteSetServerToRecordMappings())
				.build();
		
		TransactionStatus transStatus = new TransactionStatus(trans);
		transStatus.twoPC = transaction.getTwoPC();

		if(trans.getReadSet()!= null && trans.getReadSet().getElementsCount() > 0){
			for(PartitionServerElementProto partitionServer : trans.getReadSetServerToRecordMappings().getPartitionServerElementList())
			{
				NodeProto dest = partitionServer.getPartitionServer().getHost();
				//Fix me: just  send trans id
				sendClientReadLockMessage(dest, trans, partitionServer.getElements());
			}
			//Fix me: check why this block is req
			/*	TransactionProto updatedTrans = TransactionProto.newBuilder()
					.setTransactionID(transaction.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ACTIVE)
					.setWriteSet(transaction.getWriteSet())
					.setReadSetServerToRecordMappings(transaction.getReadSetServerToRecordMappings())
					.setWriteSetServerToRecordMappings(transaction.getWriteSetServerToRecordMappings())
					.build();*/
			//transStatus.trans = updatedTrans;
		}
		else{
			transStatus.isReadLockAcquired = true;
			uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
			initiateWritePhase(trans, transaction.getTwoPC());
		}
		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
	}

	/**
	 * Method used to send read lock message to participant leader
	 * @param dest
	 * @param transaction
	 * @param elements
	 */
	private void sendClientReadLockMessage(NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(elements)
				.build();

		ClientOpMsg msg = new ClientOpMsg(transClient, trans, ClientOPMsgType.READ);
		SendClientOpMessage(msg, dest);
	}

	/**
	 * Method to process incoming read response
	 * @param message
	 */
	private void handleReadResponse(ClientOpMsg message)
	{
		NodeProto source = message.getSource();
		String uid = message.getTransaction().getTransactionID();
		TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
		AddLogEntry("Msg (Read Response) received "+message+"\n");

		if(transStatus.state == TransactionType.ABORT)
		{
			AddLogEntry("Transaction has already been aborted. Requesting release of resources");
			TransactionProto releaseResourceTrans = TransactionProto.newBuilder()
					.setTransactionID(transStatus.trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ABORTED)
					.setReadSet(message.getTransaction().getReadSet())
					.build();
			ClientOpMsg msg = new ClientOpMsg(transClient, releaseResourceTrans, ClientOPMsgType.RELEASE_RESOURCE);
			SendClientOpMessage(msg, source);
			return;
		}

		if(!message.isReadLockSet())
		{
			AddLogEntry("Cannot acquire read Locks. Aborting the transaction ");
			transStatus.state = TransactionType.ABORT;
			uidTransactionStatusMap.put(uid, transStatus);

			TransactionProto transResponse = TransactionProto.newBuilder()
					.setTransactionID(transStatus.trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ABORTED)
					.setReadSet(transStatus.trans.getReadSet())
					.build();
			AddLogEntry("Sending Abort msg to user client");
			ClientOpMsg msg = new ClientOpMsg(transClient, transResponse, ClientOPMsgType.ABORT);
			SendClientResponse(clientMappings.get(uid), msg);
			return;
		}

		if(transStatus.readLocks.get(source) == false){
			transStatus.readLocks.put(source, true);
			transStatus.noOfReadLocks++;
			ElementsSetProto.Builder updatedReadSet = ElementsSetProto.newBuilder();
			updatedReadSet.addAllElements(transStatus.trans.getReadSet().getElementsList())
			.addAllElements(message.getTransaction().getReadSet().getElementsList());

			TransactionProto updatedTrans = TransactionProto.newBuilder()
					.setTransactionID(transStatus.trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ACTIVE)
					.setReadSet(updatedReadSet.build())
					.setWriteSet(transStatus.trans.getWriteSet())
					.setReadSetServerToRecordMappings(transStatus.trans.getReadSetServerToRecordMappings())
					.setWriteSetServerToRecordMappings(transStatus.trans.getWriteSetServerToRecordMappings())
					.build();

			transStatus.trans = updatedTrans;
			uidTransactionStatusMap.put(uid, transStatus);
			if(transStatus.noOfReadLocks == transStatus.readLocks.size())
			{
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				transStatus.isReadLockAcquired = true;
				uidTransactionStatusMap.put(uid, transStatus);
				if(transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount() != 0){
					initiateWritePhase(transStatus.trans, transStatus.twoPC);
				}
				else{

					updatedTrans = TransactionProto.newBuilder()
							.setTransactionID(transStatus.trans.getTransactionID())
							.setTransactionStatus(TransactionStatusProto.ACTIVE)
							.setReadSet(transStatus.trans.getReadSet())
							.build();
					transStatus.trans = updatedTrans;

					uidTransactionStatusMap.put(uid, transStatus);
					AddLogEntry("Read only Transaction completed", Level.INFO);
					releaseLocks(transStatus);
					TransactionProto transResponse = TransactionProto.newBuilder()
							.setTransactionID(transStatus.trans.getTransactionID())
							.setTransactionStatus(TransactionStatusProto.COMMITTED)
							.setReadSet(transStatus.trans.getReadSet())
							.build();

					ClientOpMsg msg = new ClientOpMsg(transClient, transResponse, ClientOPMsgType.COMMIT);
					AddLogEntry("Sending client response "+msg);
					SendClientResponse(clientMappings.get(transResponse.getTransactionID()), msg);
				}
			}

		}
	}

	/**
	 * Method used to initiate Write Phase
	 * @param trans
	 * @param twoPC
	 */
	private void initiateWritePhase(TransactionProto trans, NodeProto twoPC)
	{
		sendTwoPCInitMessagetoTPC( twoPC, trans);
		try {
			System.out.println("Sleeping for 2 secs ::: ");
			Thread.sleep(2000);
			System.out.println("Woke up from sleep. Initiating TPC among all participants");
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for(PartitionServerElementProto partitionServer : trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
		{
			NodeProto dest = partitionServer.getPartitionServer().getHost();
			sendClientWriteMessage(twoPC, dest, trans, partitionServer.getElements());
		}

	}

	/**
	 * Method used to send Client Write msg to participants
	 * @param source
	 * @param dest
	 * @param transaction
	 * @param elements
	 */
	private void sendClientWriteMessage(NodeProto source, NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setWriteSet(elements)
				.build();
		
		ClientOpMsg msg = new ClientOpMsg(source, trans , ClientOPMsgType.WRITE);
		AddLogEntry("Sending Client Write Request "+msg+" to "+dest.getHost()+":"+dest.getPort()+"\n");
		SendClientOpMessage(msg, dest);
	}

	/**
	 * Method to send transaction info to TwoPC which drives the TPC 
	 * @param dest
	 * @param transaction
	 */
	private void sendTwoPCInitMessagetoTPC(NodeProto dest, TransactionProto transaction )
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(transaction.getReadSet())
				.setWriteSet(transaction.getWriteSet())
				.setReadSetServerToRecordMappings(transaction.getReadSetServerToRecordMappings())
				.setWriteSetServerToRecordMappings(transaction.getWriteSetServerToRecordMappings())
				.build();
		
		TwoPCMsg msg = new TwoPCMsg(transClient, trans, TwoPCMsgType.INFO);
		AddLogEntry("Sending Transaction Init msg to TPC "+msg+"\n");
		SendTwoPCInitMessage(msg, dest);
	}

	/**
	 * Method to send TwoPC init message to TPC
	 * @param message
	 * @param dest
	 */
	private void SendTwoPCInitMessage(TwoPCMsg message, NodeProto dest)
	{
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	/**
	 * Method used to release Locks
	 * @param transStatus
	 */
	private void releaseLocks(TransactionStatus transStatus)
	{
		TransactionProto trans = transStatus.trans;

		for(PartitionServerElementProto partitionServer : trans.getReadSetServerToRecordMappings().getPartitionServerElementList())
		{
			NodeProto dest = partitionServer.getPartitionServer().getHost();
			sendCommitedMessage(dest, trans, partitionServer.getElements());
		}	
	}

	/**
	 * Method to processs TPC message from the TwoPC
	 * @param msg
	 */
	private void handleTwoPCResponse(TwoPCMsg msg)
	{
		//FIX ME
		/*	TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(msg.getTransaction().getTransactionID())
				.setTransactionStatus(msg.getTransaction().getTransactionStatus())
				.setReadSet(msg.getTransaction().getReadSet())
				.setWriteSet(msg.getTransaction().getWriteSet())
				.build();*/
		AddLogEntry("Received TwoPC Response : "+msg);
		ClientOpMsg message = null;
		if(msg.getMsgType() == TwoPCMsgType.COMMIT)
			message = new ClientOpMsg(transClient, msg.getTransaction(), ClientOPMsgType.COMMIT);
		else 
			message = new ClientOpMsg(transClient, msg.getTransaction(), ClientOPMsgType.ABORT);
		SendClientResponse(clientMappings.get(msg.getTransaction().getTransactionID()), message);
	}

	/**
	 * Method used to send Commit response to user client
	 * @param dest
	 * @param transaction
	 * @param elements
	 */
	private void sendCommitedMessage(NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(elements)
				.build();

		ClientOpMsg msg = new ClientOpMsg(transClient, trans, ClientOPMsgType.UNLOCK);
		SendClientOpMessage(msg, dest);
	}

	/**
	 * Method to process acks for the txn
	 * @param message
	 */
	private void ProcessClientOpMessage(ClientOpMsg message)
	{
		AddLogEntry("Received Client Response :"+message);
	}

	
	/**
	 * Method used to send ClientopMsg to user client
	 * @param message
	 * @param dest
	 */
	private void SendClientOpMessage(ClientOpMsg message, NodeProto dest)
	{	
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	/**
	 * Method to send client response
	 * @param dest
	 * @param message
	 */
	private void SendClientResponse(NodeProto dest,ClientOpMsg message)
	{
		AddLogEntry("Sending Client Response :: "+message+"to "+dest.getHost()+":"+dest.getPort()+"\n");
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 3)
		{
			System.out.println("ClientID port IsNewLog ");
		}

		int port = Integer.parseInt(args[1]);
		boolean isNew = Boolean.parseBoolean(args[2]);
		TransClient client = new TransClient(args[0], port, isNew);
		new Thread(client).start();
	}

}
