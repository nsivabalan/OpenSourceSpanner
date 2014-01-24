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
import spanner.message.BcastMsg;
import spanner.message.ClientOpMsg;
import spanner.message.MetaDataMsg;
import spanner.message.TwoPCMsg;
import spanner.metadataservice.MetaDS;
import spanner.node.Acceptor.TransactionStatus;
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
	NodeProto clientNode;
	HashMap<String, NodeProto> clientMappings ;
	
	public TransClient(String clientID, int port) throws IOException
	{
		super(clientID);
		context = ZMQ.context(1);
		InetAddress addr = InetAddress.getLocalHost();
		clientNode = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		System.out.println(" Listening to "+Common.getLocalAddress(port));
		socket.bind(Common.getLocalAddress(port));
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




	private void handleMetaDataRequest(MetaDataMsg msg)
	{
		String uid = java.util.UUID.randomUUID().toString();
		clientMappings.put(uid, msg.getSource());
		MetaDataMsg message = new MetaDataMsg(clientNode, msg.getReadSet(), msg.getWriteSet(), MetaDataMsgType.REQEUST, uid);
		sendMetaDataMsg(message);
	}
	
	/*private void initiateTrans(HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
	{
		MetaDataMsg msg = new MetaDataMsg(clientNode, readSet, writeSet, MetaDataMsgType.REQEUST);
		sendMetaDataMsg(msg);
	}*/

	private void sendMetaDataMsg(MetaDataMsg msg)
	{
		System.out.println("Sending Client Request "+msg);
		socketPush = context.socket(ZMQ.PUSH);
		//System.out.println(" "+metadataService.getHost()+":"+metadataService.getPort());
		socketPush.connect("tcp://"+metadataService.getHost()+":"+metadataService.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}
	
	private void handleMetaDataResponse(MetaDataMsg msg)
	{
		TransactionMetaDataProto transaction = msg.getTransaction();
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(transaction.getReadSet())
				.setWriteSet(transaction.getWriteSet())
				.setReadSetServerToRecordMappings(transaction.getReadSetServerToRecordMappings())
				.setWriteSetServerToRecordMappings(transaction.getWriteSetServerToRecordMappings())
				.build();
		System.out.println("Trans details "+trans);
		System.out.println(" Chosen TPC <<<<<< "+transaction.getTwoPC());
		TransactionStatus transStatus = new TransactionStatus(trans);
		transStatus.twoPC = transaction.getTwoPC();

		if(trans.getReadSet()!= null && trans.getReadSet().getElementsCount() > 0){
			for(PartitionServerElementProto partitionServer : trans.getReadSetServerToRecordMappings().getPartitionServerElementList())
			{
				NodeProto dest = partitionServer.getPartitionServer().getHost();
				sendClientReadLockMessage(dest, trans, partitionServer.getElements());
			}
			TransactionProto updatedTrans = TransactionProto.newBuilder()
					.setTransactionID(transaction.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ACTIVE)
					.setWriteSet(transaction.getWriteSet())
					.setReadSetServerToRecordMappings(transaction.getReadSetServerToRecordMappings())
					.setWriteSetServerToRecordMappings(transaction.getWriteSetServerToRecordMappings())
					.build();
			transStatus.trans = updatedTrans;
		}
		else{
			transStatus.isReadLockAcquired = true;
			uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
			initiateWritePhase(trans, transaction.getTwoPC());
		}
		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
	}
	
	private void sendClientReadLockMessage(NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(elements)
				.build();

		ClientOpMsg msg = new ClientOpMsg(clientNode, trans, ClientOPMsgType.READ);
		SendClientOpMessage(msg, dest);
	}

	
	private void sendCommitedMessage(NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setReadSet(elements)
				.build();

		ClientOpMsg msg = new ClientOpMsg(clientNode, trans, ClientOPMsgType.UNLOCK);
		SendClientOpMessage(msg, dest);
	}
	
	private void SendClientOpMessage(ClientOpMsg message, NodeProto dest)
	{
		System.out.println("Sending Client Request "+message);
		socketPush = context.socket(ZMQ.PUSH);
		System.out.println("Destination ::::: "+dest.getHost()+":"+dest.getPort());
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	public void run()
	{
		System.out.println("Waiting for messages "+socket.toString());
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			//System.out.println("Received Messsage "+receivedMsg);
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
							ProcessReadResponse(message);
						}
						else{
							ProcessClientOpMessage(message);
						}
					}
					else if (msgwrap.getmessageclass() == TwoPCMsg.class)
					{
						TwoPCMsg message = (TwoPCMsg)msgwrap.getDeSerializedInnerMessage();
						System.out.println("Client Response "+message);
						handleTwoPCResponse(message);
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		this.close();
		socket.close();
		context.term();
	}

	private void ProcessReadResponse(ClientOpMsg message)
	{
		NodeProto source = message.getSource();
		String uid = message.getTransaction().getTransactionID();
		TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
		
		if(transStatus.state == TransactionType.ABORT)
		{
			System.out.println("Transaction has already been aborted. Requesting release of resources");
			TransactionProto releaseResourceTrans = TransactionProto.newBuilder()
					.setTransactionID(transStatus.trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ABORTED)
					.setReadSet(message.getTransaction().getReadSet())
					.build();
			ClientOpMsg msg = new ClientOpMsg(clientNode, releaseResourceTrans, ClientOPMsgType.RELEASE_RESOURCE);
			SendClientOpMessage(msg, source);
			return;
		}
		
		if(!message.isReadLockSet())
		{
			System.out.println("Cannot acquire read Locks. Aborting the transaction ");
			transStatus.state = TransactionType.ABORT;
			uidTransactionStatusMap.put(uid, transStatus);
			
			TransactionProto transResponse = TransactionProto.newBuilder()
					.setTransactionID(transStatus.trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ABORTED)
					.setReadSet(transStatus.trans.getReadSet())
					.build();
			
			ClientOpMsg msg = new ClientOpMsg(clientNode, transResponse, ClientOPMsgType.ABORT);
			SendClientResponse(clientMappings.get(uid), msg);
			return;
		}
		
		if(transStatus.readLocks.get(source) == false){
			transStatus.readLocks.put(source, true);
			transStatus.noOfReadLocks++;
			ElementsSetProto.Builder updatedReadSet = ElementsSetProto.newBuilder();
			/*System.out.println("Processing Read Response from "+source);
			System.out.println("Read Response READ SET :::::: "+message.getTransaction().getReadSet());
			System.out.println("Old read SET ::::: "+transStatus.trans.getReadSet());
			*/
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
					System.out.println("Read only Transaction completed");
				//	System.out.println(" "+transStatus.trans);
					
					releaseLocks(transStatus);
					
					TransactionProto transResponse = TransactionProto.newBuilder()
							.setTransactionID(transStatus.trans.getTransactionID())
							.setTransactionStatus(TransactionStatusProto.COMMITTED)
							.setReadSet(transStatus.trans.getReadSet())
							.build();
					
					ClientOpMsg msg = new ClientOpMsg(clientNode, transResponse, ClientOPMsgType.COMMIT);
					SendClientResponse(clientMappings.get(transResponse.getTransactionID()), msg);
				}
			}

		}
	}

	private void releaseLocks(TransactionStatus transStatus)
	{
		TransactionProto trans = transStatus.trans;
		
		for(PartitionServerElementProto partitionServer : trans.getReadSetServerToRecordMappings().getPartitionServerElementList())
		{
			NodeProto dest = partitionServer.getPartitionServer().getHost();
			sendCommitedMessage(dest, trans, partitionServer.getElements());
		}	
	}
	
	private void initiateWritePhase(TransactionProto trans, NodeProto twoPC)
	{
		sendTwoPCInitMessagetoTPC( twoPC, trans);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(PartitionServerElementProto partitionServer : trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
		{
			NodeProto dest = partitionServer.getPartitionServer().getHost();
			sendClientWriteMessage(twoPC, dest, trans, partitionServer.getElements());
		}

	}

	private void sendClientWriteMessage(NodeProto source, NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ACTIVE)
				.setWriteSet(elements)
				.build();

		ClientOpMsg msg = new ClientOpMsg(source, trans , ClientOPMsgType.WRITE);
		SendClientOpMessage(msg, dest);
	}

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
		System.out.println("Sending Trans Init msg to TPC RS :: "+transaction.getReadSet()+"\n WS :: "+transaction.getWriteSet());
		TwoPCMsg msg = new TwoPCMsg(clientNode, trans, TwoPCMsgType.INFO);
		SendTwoPCInitMessage(msg, dest);
	}

	
	private void SendClientResponse(NodeProto dest,ClientOpMsg message)
	{
		System.out.println("Sending Client Response ::: "+message+"\n to "+dest);
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}
	
	
	private void handleTwoPCResponse(TwoPCMsg msg)
	{
	/*	TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(msg.getTransaction().getTransactionID())
				.setTransactionStatus(msg.getTransaction().getTransactionStatus())
				.setReadSet(msg.getTransaction().getReadSet())
				.setWriteSet(msg.getTransaction().getWriteSet())
				.build();*/
		ClientOpMsg message = new ClientOpMsg(clientNode, msg.getTransaction(), ClientOPMsgType.COMMIT);
		SendClientResponse(clientMappings.get(msg.getTransaction().getTransactionID()), message);
	}
	
	private void SendTwoPCInitMessage(TwoPCMsg message, NodeProto dest)
	{
		System.out.println("Sending Client Request "+message);
		socketPush = context.socket(ZMQ.PUSH);
		System.out.println(" "+dest.getHost()+":"+dest.getPort());
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	private void ProcessClientOpMessage(ClientOpMsg message)
	{
		System.out.println("Received Client Response "+message.getSource());
		System.out.println(""+message.toString());
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 2)
		{
			System.out.println("ClientID port ");
		}

		int port = Integer.parseInt(args[1]);
		TransClient client = new TransClient(args[0], port);
		new Thread(client).start();
	}

}
