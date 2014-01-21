package spanner.node;

import java.io.IOException;
import java.sql.Timestamp;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.Resource;
import spanner.common.Common.BcastMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.State;
import spanner.common.MessageWrapper;
import spanner.common.ResourceHM;
import spanner.message.BcastMsg;
import spanner.message.PaxosMsg;


public class Acceptor extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;

	ZMQ.Socket socket = null;
	private Map<UUID, TransactionStatus> uidTransactionStatusMap;
	private static ResourceHM localResource = null;
	
	final class TransactionStatus {
		HashMap<String, HashMap<String, String>> data;
		Common.AcceptorState state;
		Set<String> Acceptors;	
		Timestamp timeout;
	}
	
	public Acceptor(String shard, String replicaID) throws IOException
	{
		super(replicaID);
		context = ZMQ.context(1);
		String hostport = Common.getProperty(replicaID);
		String[] hostDetails = hostport.split(":");
		
		String shardDetails = Common.getProperty(shard);
		String[] shards = shardDetails.split(";");
		System.out.println("Shards "+shards[0]+" "+shards[1]);
		String subPort = Common.getProperty(shard+"Leader");
		String[] leaderDetails = Common.getProperty(shards[0]).split(":");
				
		subscriber = context.socket(ZMQ.SUB);
		subscriber.connect(Common.getAddress(leaderDetails[0], Integer.parseInt(subPort)));
		
		subscriber.subscribe("".getBytes());
		
		socket = context.socket(ZMQ.PUSH);
		socket.connect(Common.getAddress(leaderDetails[0], Integer.parseInt(leaderDetails[1])));
		uidTransactionStatusMap = new HashMap<UUID, TransactionStatus>();
		//localResource = new Resource(this.LOGGER);
		localResource = new ResourceHM(this.LOGGER);
		
		
		/*
		String paxosLeaderAddress = getAddress(leaderDetails[0], Integer.parseInt(leaderDetails[1]));
		MessageBroker broker = new MessageBroker(paxosLeaderAddress, localAddress);
		new Thread(broker).start();*/
	}
	
	

	public void run() 
	{
		while(!Thread.currentThread ().isInterrupted ()){
			byte[] response = subscriber.recv();
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(new String( response));
			if (msgwrap != null)
			{
				try {
					if (msgwrap.getmessageclass() == BcastMsg.class  && this.NodeState == State.ACTIVE)
					{
						
						BcastMsg msg = (BcastMsg) msgwrap.getDeSerializedInnerMessage();
						
						//Print msg
						System.out.println("Received " + msg);
						this.AddLogEntry("Received "+msg, Level.INFO);
						
					/*	if(msg.getType() == BcastMsgType.COMMIT_ACK)
							ProcessCommitAckMessage(msg.getUID(), msg.getNodeid(),msg.getData());

						else if (msg.getType() == BcastMsgType.ABORT_ACK)
							ProcessAbortAckMessage(msg.getUID(), msg.getNodeid());*/
					}

					else if (msgwrap.getmessageclass() == PaxosMsg.class  && this.NodeState == State.ACTIVE)
					{
						PaxosMsg msg = (PaxosMsg) msgwrap.getDeSerializedInnerMessage();

						//Print msg
						System.out.println("Received " + msg);
						this.AddLogEntry("Received "+msg, Level.INFO);
						
						if(msg.getType() == PaxosMsgType.ACCEPT)
							ProcessAcceptMessage(msg.getUID(), msg.getData());

						else if (msg.getType() == PaxosMsgType.COMMIT )
							ProcessCommitMessage(msg.getUID());

						else if (msg.getType() == PaxosMsgType.ABORT)
							ProcessAbortMessage(msg.getUID());
					}
					/*else if (msgwrap.getmessageclass() == SiteCrashMsg.class)
					{
						SiteCrashMsg msg = (SiteCrashMsg) msgwrap.getDeSerializedInnerMessage();
						
						//Print msg
						System.out.println("Received " + msg);
						this.AddLogEntry("Received "+msg, Level.INFO);
						
						if(msg.getType() == SiteCrashMsgType.CRASH && this.NodeState == State.ACTIVE)
						{
							this.NodeState = State.PAUSED;
						}
						else if(msg.getType() == SiteCrashMsgType.RECOVER && this.NodeState == State.PAUSED)
						{
							this.NodeState = State.ACTIVE;
						}					
					}*/
					else
					{
						//Message Discarded.
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			/*System.out.println("Received update "+new String(response));
			SendMsgToPaxosLeader("Test Response 1");*/
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		close();
		subscriber.close();
		socket.close();
		context.term();
	}
	
	
	//method used to process the accept msg for the first time when the Paxos leader sends the data
		public void ProcessAcceptMessage(UUID uid, HashMap<String, HashMap<String, String>> data) throws IOException
		{
			TransactionStatus temp = new TransactionStatus();
			
			temp.data = data;
			temp.state = Common.AcceptorState.ACCEPT;
			temp.timeout = new Timestamp(new Date().getTime());
			temp.Acceptors = new HashSet<String>();
			this.uidTransactionStatusMap.put(uid, temp);

			PaxosMsg msg = new PaxosMsg(this.nodeId, Common.PaxosMsgType.ACK, uid);
			SendMsgToPaxosLeader(msg);
		}
	
		public void ProcessCommitMessage(UUID uid) throws Exception
		{
			TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
			localResource.WriteResource(transStatus.data);
			transStatus.state = Common.AcceptorState.COMMIT;
			uidTransactionStatusMap.put(uid, transStatus);
			this.AddLogEntry("Data written to local resource "+transStatus.data, Level.INFO);
			this.AddLogEntry("Trans "+uid+" Commited ", Level.INFO);
			System.out.println("Data written to local resource "+transStatus.data);
			System.out.println("Trans "+uid+" Commited ");
			
			PaxosMsg msg = new PaxosMsg(this.nodeId, Common.PaxosMsgType.COMMIT, uid);
			SendMsgToPaxosLeader(msg);
		}
		
		public void ProcessAbortMessage(UUID uid) throws Exception
		{
			TransactionStatus transStatus = uidTransactionStatusMap.get(uid);
			transStatus.state = Common.AcceptorState.ABORT;
			uidTransactionStatusMap.put(uid, transStatus);
			this.AddLogEntry("Trans aborted "+uid, Level.INFO);
			System.out.println("Trans aborted "+uid);
		}
		
	private void SendMsgToPaxosLeader(PaxosMsg msg)
	{
		
		/*System.out.println("Sending msg to Paxos Leader one on one \""+msg+"\" ");
		socket.send(msg.getBytes (), 0);
		*/
		//Print msg
		System.out.println("Sent " + msg);
		this.AddLogEntry("Sent "+msg, Level.INFO);

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socket.send(msgwrap.getSerializedMessage().getBytes(), 0);
		
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if(args.length != 2)
			throw new IllegalArgumentException("Pass ShardID and ReplicaID");
		
		Acceptor acceptor = new Acceptor(args[0], args[1]);
		new Thread(acceptor).start();

	}

}
