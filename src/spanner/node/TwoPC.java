package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.PartitionServerElementProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;
import org.zeromq.ZMQ;
import spanner.common.Common.TwoPCMsgType;
import spanner.common.Common.TransactionType;
import spanner.common.Resource;
import spanner.common.ResourceHM;
import spanner.common.MessageWrapper;
import spanner.common.Common;

public class TwoPC extends Node implements Runnable{

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
	protected static Logger LOGGER = null;

	public TwoPC(String shard, NodeProto nodeAddress, ZMQ.Context context , boolean isNew) throws IOException
	{
		super(shard+"_TPC", isNew);
		this.context = context;
		pendingTransactions = new ArrayList<String>();
		br = new BufferedReader(new InputStreamReader(System.in));
		this.uidTransactionStatusMap = new LinkedHashMap<String, TransactionStatus>();
		this.nodeAddress = nodeAddress;
		uidTransTypeMap = new HashMap<String, TransactionType>();
		pendingTrans = new HashSet<String>();
	}

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

	/**
	 * Method to check for pending transactions. Triggers abort of the same after timeout
	 */
	private synchronized void checkForPendingTrans() 
	{
		Long curTime = new Date().getTime();
		Set<String> pendingTransTemp = pendingTrans;
		for(String uid: pendingTransTemp)
		{
			if(uidTransTypeMap.get(uid) != TransactionType.ABORT)
			{
				TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
				if(curTime - transStatus.initTimeStamp > Common.TPC_TIMEOUT)
				{	
					//AddLogEntry("Transaction timed out "+uid+"\n");
					if(uidTransTypeMap.get(uid) != TransactionType.ABORT)
					{
						AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
						AddLogEntry("Aborting the transaction "+uid+" due to time out");
						TwoPCMsg response = new TwoPCMsg(nodeAddress, transStatus.trans, TwoPCMsgType.ABORT);
						pendingTrans.remove(uid);
						uidTransTypeMap.put(uid, TransactionType.ABORT);
						transStatus.transState = TransactionType.ABORT;
						uidTransactionStatusMap.put(uid, transStatus);
						AddLogEntry("Sending Abort msg to Trans Client "+response, Level.INFO);
						SendTwoPCMessage(response, transStatus.source);
						for(PartitionServerElementProto partitionServer : transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
						{
							NodeProto dest = partitionServer.getPartitionServer().getHost();
							sendAbortInitMessage(nodeAddress, dest, transStatus.trans, partitionServer.getElements());
						}
						AddLogEntry("*************************** End of TPC module ************************** ", Level.FINE);
					}

				}
			}
		}
	}

	/**
	 * Method to process incoming INFO msg from transactional client. Updates local datastructure and waits for PREPARE ACK from participants
	 * @param message, TwoPCMsg
	 */
	public void ProcessInfoMessage(TwoPCMsg message)
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
		NodeProto transClient= message.getSource();
		TransactionProto trans = message.getTransaction();
		AddLogEntry("Received INFO msg "+message+" from "+transClient.getHost()+":"+transClient.getPort()+"\n", Level.INFO);
		TransactionStatus transStatus = new TransactionStatus(transClient, trans);
		transStatus.initTimeStamp = System.currentTimeMillis();
		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
		AddLogEntry("Waiting for PREPARE_ACK from all participants\n" , Level.INFO);
		uidTransTypeMap.put(trans.getTransactionID(), TransactionType.WRITEINIT);
		pendingTrans.add(trans.getTransactionID());
		AddLogEntry("*************************** End of TPC module ************************** ", Level.FINE);
	}

	/**
	 * Method used to process incoming PREPARE message for a two phase commit
	 * @param message
	 * @throws IOException
	 */
	public synchronized void ProcessPrepareMessage(TwoPCMsg message) throws IOException
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
		NodeProto transClient= message.getSource();
		TransactionProto trans = message.getTransaction();
		String uid = trans.getTransactionID();
		AddLogEntry("Received Prepare msg "+message+" from participant "+transClient.getHost()+":"+transClient.getPort()+"\n");
		if(pendingTrans.contains(trans.getTransactionID()))
		{
			TransactionStatus transStatus = uidTransactionStatusMap.get(trans.getTransactionID());
			if(uidTransTypeMap.get(uid) != TransactionType.ABORT){
				transStatus.paritcipantListPrepare.add(transClient);
				if(transStatus.paritcipantListPrepare.size() == transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount())
				{
					AddLogEntry("Received Prepare msg from all participants. Initiating COMMIT phase");
					for(PartitionServerElementProto partitionServer : transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
					{
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
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
				AddLogEntry("Already aborted the transaction. No action taken\n");
			}


		}
		else
			AddLogEntry("Already decision taken on the transaction. No action taken for now\n");
		AddLogEntry("*************************** End of TPC module ************************** ", Level.FINE);
	}

	/**
	 * Method to process incoming Commit message (ack for 2nd phase of TPC)
	 * @param message
	 * @throws IOException
	 */
	public synchronized void ProcessCommitMessage(TwoPCMsg message) throws IOException
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
		NodeProto participant = message.getSource();
		TransactionProto trans = message.getTransaction();
		AddLogEntry("Received Commit message "+message+"\n");
		TransactionStatus transStatus = uidTransactionStatusMap.get(trans.getTransactionID());
		String uid = trans.getTransactionID();
		if(uidTransTypeMap.get(uid) != TransactionType.ABORT){
			transStatus.paritcipantListCommit.add(participant);
			uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
			/*System.out.println("Expected Count "+transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount());
			System.out.println("Actual Count "+transStatus.paritcipantListCommit.size());
			System.out.println(" "+transStatus.paritcipantListCommit);*/

			if(transStatus.paritcipantListCommit.size() == transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount())
			{
				AddLogEntry("Received Commit Ack From all participants. Sending COMMIT response to Transactional Client");
				TransactionProto clientResponse = TransactionProto.newBuilder()
						.setTransactionID(transStatus.trans.getTransactionID())
						.setTransactionStatus(TransactionStatusProto.COMMITTED)
						.setReadSet(transStatus.trans.getReadSet())
						.setWriteSet(transStatus.trans.getWriteSet())
						.build();

				pendingTrans.remove(trans.getTransactionID());
				TwoPCMsg commit_response = new TwoPCMsg(nodeAddress, clientResponse, TwoPCMsgType.COMMIT);
				AddLogEntry("Sending Commit msg "+commit_response+" to Transactional Client "+trans.getTransactionID());
				SendTwoPCMessage(commit_response, transStatus.source);		
			}
			else{
				StringBuffer buffer = new StringBuffer();
				for(NodeProto nodeProto : transStatus.paritcipantListCommit)
					buffer.append(nodeProto.getHost()+":"+nodeProto.getPort()+", ");
				AddLogEntry("Yet to receive COMMIT ACKS from few more participants. List of received participants "+buffer.toString()+"\n");
			}
		}
		else{
			AddLogEntry("Already aborted. No action taken");
		}
		AddLogEntry("*************************** End of TPC module ************************** ", Level.FINE);
	}

	/**
	 * Method to process incoming Abort message from participants (Ack during 2nd phase of TPC)
	 * @param message
	 * @throws IOException
	 */
	public synchronized void ProcessAbortMessage(TwoPCMsg message) throws IOException
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
		NodeProto participant = message.getSource();
		TransactionProto trans = message.getTransaction();
		AddLogEntry("Received Abort msg "+message+" from "+participant.getHost()+":"+participant.getPort());
		String uid = trans.getTransactionID();
		TransactionStatus transStatus = uidTransactionStatusMap.get(trans.getTransactionID());
		if(uidTransTypeMap.get(uid) != TransactionType.ABORT){
			AddLogEntry("Received abort message from one participant for first time. Aborting the transaction ", Level.FINE);
			transStatus.transState = TransactionType.ABORT;
			transStatus.paritcipantListAbort.add(participant);
			uidTransTypeMap.put(uid,  TransactionType.ABORT);
			transStatus.transState = TransactionType.ABORT;
			uidTransactionStatusMap.put(uid, transStatus);
			TwoPCMsg abort_response = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
			if(pendingTrans.contains(uid))
				pendingTrans.remove(trans.getTransactionID());
			SendTwoPCMessage(abort_response, transStatus.source);		
			AddLogEntry("Sent Abort msg "+abort_response+"to Trans Client- "+trans.getTransactionID());	

			for(PartitionServerElementProto partitionServer : transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementList())
			{
				NodeProto dest = partitionServer.getPartitionServer().getHost();
				if(!dest.equals(participant))
					sendAbortInitMessage(nodeAddress, dest, trans, partitionServer.getElements());
			}
		}
		else{
			transStatus.paritcipantListAbort.add(participant);
			AddLogEntry("Abort initiated already for the transaction. Updating the Abort Acks");
			AddLogEntry("Expected Count "+transStatus.trans.getWriteSetServerToRecordMappings().getPartitionServerElementCount()+"\n", Level.FINE);
			AddLogEntry("Actual Count "+transStatus.paritcipantListAbort.size()+"\n", Level.FINE);
		}
		uidTransactionStatusMap.put(trans.getTransactionID(), transStatus);
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
	}

	/**
	 * Method used to initiate Commit phase in Two Phase Commit among all participants
	 * @param source
	 * @param dest
	 * @param transaction
	 * @param elements
	 */
	private void sendCommitInitMessage(NodeProto source, NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.COMMITTED)
				.setWriteSet(elements)
				.build();

		TwoPCMsg msg = new TwoPCMsg(source, trans , TwoPCMsgType.COMMIT);
		SendCommitInitMessage(msg, dest);
		AddLogEntry("*************************** End of  TPC  module ************************** ", Level.FINE);
	}

	/**
	 * Method to send Commit message to all participants
	 * @param message
	 * @param dest
	 */
	private void SendCommitInitMessage(TwoPCMsg message, NodeProto dest)
	{			
		AddLogEntry("Sent Commit Init msg "+message+" to Participant- "+dest.getHost()+":"+dest.getPort());
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	/**
	 * Method used to initiate Abort during second phase of Two Phase Commit among all participants
	 * @param source
	 * @param dest
	 * @param transaction
	 * @param elements
	 */
	private void sendAbortInitMessage(NodeProto source, NodeProto dest, TransactionProto transaction , ElementsSetProto elements)
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
		TransactionProto trans = TransactionProto.newBuilder()
				.setTransactionID(transaction.getTransactionID())
				.setTransactionStatus(TransactionStatusProto.ABORTED)
				.setWriteSet(elements)
				.build();
		TwoPCMsg msg = new TwoPCMsg(source, trans , TwoPCMsgType.ABORT);
		AddLogEntry("Sending Abort Msg "+msg+" to participant "+dest.getHost()+":"+dest.getPort());
		SendTwoPCMessage(msg, dest);
		AddLogEntry("*************************** End of TPC module ************************** ", Level.FINE);
	}

	/**
	 * Method to send TwoPC message to Transactional client
	 * @param message
	 * @param dest
	 */
	private void SendTwoPCMessage(TwoPCMsg message, NodeProto dest)
	{
		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
	}

	/*
	public void ProcessClientReadMessage(ClientOpMsg message)
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
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
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
	}

	public void ProcessClientWriteMessage(ClientOpMsg message)
	{
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
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
		AddLogEntry("*************************** Start of TPC module ************************** ", Level.FINE);
	}
	 */

	/**
	 * Method to send ClientOpMsg to User Client
	 * @param message
	 * @param dest
	 */
	/*private void SendClientMessage(ClientOpMsg message, NodeProto dest)
	{
		System.out.println("Sending ClientOpMsg " + message+" to "+dest.getHost()+":"+dest.getPort());
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0 );

	}
	 */



}
