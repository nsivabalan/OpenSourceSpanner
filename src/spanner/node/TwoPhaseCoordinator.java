package spanner.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import javax.swing.plaf.basic.BasicBorders.ToggleButtonBorder;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Resource;
import spanner.common.Common.PaxosLeaderState;
import spanner.message.ClientOpMsg;
import spanner.message.TwoPCMsg;
import spanner.node.PaxosLeader.TransactionStatus;

public class TwoPhaseCoordinator extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	ArrayList<UUID> pendingTransactions = null;
	int port;

	final class TransactionStatus {

		Common.TransactionType state;
		HashMap<String, HashMap<String, String>> readSet;
		HashMap<String, HashMap<String, String>> writeSet;
		HashMap<String, HashMap<String, String>> data;
		String clientRoutingKey;
		Boolean isCommitAckReceived;
	    Boolean isReadLockAcquired;
	    int readLockSoFor;
	    int writeSetAcceptedSoFor;
	    int expectedReadLockCount;
	    int expectedWriteParticipantsCount;
		Set<String> readAcceptorListPrepare;
		Set<String> readAcceptorListCommit;
		Set<String> readAcceptorListAbort;
		Set<String> writeAcceptorListInfo;
		Set<String> writeAcceptorListPrepare;
		Set<String> writeAcceptorListCommit;
		Set<String> writeAcceptorListAbort;

		public TransactionStatus(HashMap<String, HashMap<String, String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
		{			
			this.readAcceptorListPrepare = new HashSet<String>();	
			this.readAcceptorListCommit =  new HashSet<String>();
			this.readAcceptorListAbort =  new HashSet<String>();
			this.writeAcceptorListCommit = new HashSet<String>();
			this.writeAcceptorListPrepare = new HashSet<String>();
			this.writeAcceptorListAbort = new HashSet<String>();
			
			this.writeSet = writeSet;
			this.readSet = readSet;
			this.isCommitAckReceived = false;
			this.isReadLockAcquired = false;
		}		
	}


	private Map<UUID, TransactionStatus> uidTransactionStatusMap;
	HashMap<String , String> shardLocator = null;
	
	public TwoPhaseCoordinator(String shardId, int port, ArrayList<String> shardIds) throws IOException
	{
		super(shardId);
		context = ZMQ.context(1);

		ZMQ.Context context = ZMQ.context(1);

		shardLocator = new HashMap<String, String>();
		for(String shard : shardIds){
			String shardDetails = Common.getProperty(shard);
			String[] shards = shardDetails.split(",");
			String subPort = Common.getProperty(shard+"Leader");
			shardLocator.put(shard, Common.getProperty(shards[0]));
		}

		socket = context.socket(ZMQ.PULL);

		socket.connect(Common.getLocalAddress(port));
		this.port = port;
		
		
		
		pendingTransactions = new ArrayList<UUID>();

		this.uidTransactionStatusMap = new LinkedHashMap<UUID, TransactionStatus>();
		
	}
	
	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(new String( socket.recv(0)));
			if (msgwrap != null)
			{
				try {
					if (msgwrap.getmessageclass() == ClientOpMsg.class)
					{
						ClientOpMsg message = (ClientOpMsg)msgwrap.getDeSerializedInnerMessage();
						ProcessClientOpMessage(message);
					}
					else if(msgwrap.getmessageclass() == TwoPCMsg.class )
					{
						TwoPCMsg msg = (TwoPCMsg) msgwrap.getDeSerializedInnerMessage();

						//Print msg
						System.out.println("Received " + msg);
						this.AddLogEntry("Received - "+msg, Level.INFO);
						
						if (msg.getMsgType() == Common.TwoPCMsgType.INFO)
							//ProcessInfoRequest(msg);
							System.out.println("TODO ");

						if(msg.getMsgType() == Common.TwoPCMsgType.PREPARE)
							//ProcessPrepareMessage(msg);
							System.out.println("TODO ");
						else if (msg.getMsgType() == Common.TwoPCMsgType.COMMIT)
							//ProcessCommitAck(msg);
							System.out.println("TODO ");

						else if (msg.getMsgType() == Common.TwoPCMsgType.ABORT)
							//ProcessAbortAck(msg);
							System.out.println("TODO ");

					}
					
					
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		this.close();
		socket.close();
		context.term();
	}
	
	private void ProcessClientOpMessage(ClientOpMsg message)
	{
		//Client Op message will not reach TPC. If trans involved only one PL, client will handle it. 
		//Else, client sends a TwoPCMsg to the TPC
	}
	
	/*private void ProcessInfoRequest(TwoPCMsg message)
	{
		TransactionStatus transStatus = null;
		if(uidTransactionStatusMap.containsKey(message.getUID()))
			transStatus = uidTransactionStatusMap.get(message.getUID());
		else
			transStatus = new TransactionStatus(message.getReadSet(), message.getData());
		
		transStatus.data.putAll(message.getData());
		transStatus.readSet = message.getReadSet();
		
		for(String paxLeader: message.getPaxosLeadersList())
			transStatus.writeAcceptorListInfo.add(paxLeader);
		
		transStatus.clientRoutingKey = message.getClientRoutingKey();
		
		transStatus.expectedWriteParticipantsCount = transStatus.writeAcceptorListPrepare.size();
		
		transStatus.isReadLockAcquired = true;
		//nothing to be called from here. expecting responses from Paxos Leaders 
		
	}
	
	
	private void ProcessPrepareMessage(TwoPCMsg message)
	{
		TransactionStatus transStatus = uidTransactionStatusMap.get(message.getUID());
		if(message.getType() == Common.TwoPCMsgType.ABORT)
		{
			return;
		}
		transStatus.writeAcceptorListPrepare.add(message.getNodeid());
		if (transStatus.writeAcceptorListPrepare.size() == transStatus.expectedWriteParticipantsCount)
		{
			System.out.println("Preparing Commit");
			StringBuffer sb1 = new StringBuffer();
			sb1.append("\nUID - " + message.getUID());
			sb1.append("\nInitiating Commit.");
			this.AddLogEntry(sb1.toString(), Level.INFO);
			
			transStatus.state = Common.TransactionType.COMMIT;
			
			this.uidTransactionStatusMap.put(message.getUID(), transStatus);
			SendCommitMessage(message.getUID());
		}
		
	}
	
	private void ProcessCommitAck(TwoPCMsg message)
	{
		TransactionStatus transStatus = uidTransactionStatusMap.get(message.getUID());
	
		transStatus.writeAcceptorListCommit.add(message.getNodeid());
		if (transStatus.writeAcceptorListCommit.size() == transStatus.expectedWriteParticipantsCount)
		{
			System.out.println("Committed by all participants");
			StringBuffer sb1 = new StringBuffer();
			sb1.append("\nUID - " + message.getUID());
			sb1.append("\nCommit Done");
			this.AddLogEntry(sb1.toString(), Level.INFO);
		}
	}
	
	private void ProcessAbortAck(TwoPCMsg message)
	{
		TransactionStatus transStatus = uidTransactionStatusMap.get(message.getUID());
		transStatus.writeAcceptorListAbort.add(message.getNodeid());
		if (transStatus.writeAcceptorListAbort.size() == transStatus.expectedWriteParticipantsCount)
		{
			System.out.println("Aborted by all participants");
			StringBuffer sb1 = new StringBuffer();
			sb1.append("\nUID - " + message.getUID());
			sb1.append("\nAbort Done");
			this.AddLogEntry(sb1.toString(), Level.INFO);
		}
	}
	
	private void SendCommitMessage(UUID uid)
	{
		TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
		for(String shard: transStatus.writeAcceptorListInfo)
		{
			TwoPCMsg message = new TwoPCMsg(uid, Common.TwoPCMsgType.COMMIT);
			SendMessageToPaxosLeader(message, shard);
		}
	}
	
	private void SendMessageToPaxosLeader(TwoPCMsg message, String shard)
	{
		System.out.println("Sending TwoPC Message "+message);
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect(shardLocator.get(shard));

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
		
	}*/
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
