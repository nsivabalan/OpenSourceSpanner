package spanner.node;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.ResourceHM;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PLeaderState;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosLeaderState;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.TwoPCMsgType;
import spanner.locks.LockTableOld;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosDetailsMsg;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;
import spanner.node.Participant.TransactionStatus;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ColElementProtoOrBuilder;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProtoOrBuilder;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

public class PAcceptor_Mod extends Node implements Runnable{

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
	LockTableOld lockTable = null;
	private static Logger PAXOSLOG = null;
	int acceptorsCount = 0;
	BallotNumber ballotNo = null;
	private HashMap<Integer, LogPositionWritten> logLocationtoTransIdMap = null; 
	private HashMap<String, PaxosInstance> uidPaxosInstanceMap = null;
	private HashSet<String> pendingPaxosInstances = null;
	private HashMap<String, TransactionSource> uidTransMap = null;
	private boolean isValueDecided ;
	TwoPC twoPhaseCoordinator = null;
	private int logCounter = 0;
	RandomAccessFile logRAF = null;
	private static ResourceHM localResource = null;

	private int acceptedValue = -1;
	public PAcceptor_Mod(String shard, String nodeId) throws IOException
	{
		super(nodeId);
		this.shard = shard;
		context = ZMQ.context(1);

		ZMQ.Context context = ZMQ.context(1);


		LOGGER = Logger.getLogger(nodeId);
		String[] hostDetails = Common.getProperty(nodeId).split(":");
		// Socket to receive messages on
		System.out.println("Receiving message "+Common.getLocalAddress(Integer.parseInt(hostDetails[1])));
		socket = context.socket(ZMQ.PULL); 
		twoPhaseCoordinator = new TwoPC(nodeAddress, context);
		
		//create Log file
		createLogFile(shard, nodeId);

		//socket.connect(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));

		//socket.bind(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket.bind("tcp://127.0.0.1:"+hostDetails[1]);
		InetAddress addr = InetAddress.getLocalHost();
		nodeAddress = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(Integer.parseInt(hostDetails[1])).build();
		System.out.println("Participant node address ****** "+nodeAddress);
		String[] mds = Common.getProperty("mds").split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		lockTable = new LockTableOld();
		pendingPaxosInstances = new HashSet<String>();
		uidTransMap = new HashMap<String, TransactionSource>();
		localResource = new ResourceHM(this.LOGGER);
		//to fix: remove after testing
		if(nodeId.equalsIgnoreCase("1") || nodeId.equalsIgnoreCase("4"))
			isLeader = true;

		state = PLeaderState.INIT;
		//state = PLeaderState.ACTIVE;
		myId = Integer.parseInt(nodeId);
		ballotNo = new BallotNumber(0, myId);
		isValueDecided = false;
		uidPaxosInstanceMap = new HashMap<String, PaxosInstance>();
		logLocationtoTransIdMap = new HashMap<Integer, LogPositionWritten>();
		sendPaxosMsgRequestingAcceptors();
	}

	private class LogPositionWritten{
		String transId;
		boolean isWritten;

		public LogPositionWritten(String transId)
		{
			this.transId = transId;
		}
		public boolean isWritten() {
			return isWritten;
		}
		public void setWritten(boolean isWritten) {
			this.isWritten = isWritten;
		}

	}

	private class TransactionSource{
		TransactionProto trans;
		TwoPCMsgType type;
		NodeProto source;
		public TransactionSource(TransactionProto trans, NodeProto source, TwoPCMsgType type)
		{
			this.trans = trans;
			this.source = source;
			this.type = type;
		}
		
		public TwoPCMsgType getType()
		{
			return this.type;
		}
		
		public NodeProto getSource()
		{
			return this.source;
		}
		public TransactionProto getTrans()
		{
			return this.trans;
		}
	}
	private void createLogFile(String shard, String nodeId) throws NumberFormatException, IOException
	{
		File file = new File(Common.PaxosLog+"/"+shard+"/"+nodeId+"_.log");
		if(file.exists())
		{

			PAXOSLOG = Logger.getLogger(Common.PaxosLog+"/"+shard+"/"+nodeId+"_.log");


		}
		else{

			PAXOSLOG = Logger.getLogger(Common.PaxosLog+"/"+shard+"/"+nodeId+"_.log");


		}	
	}

	private void writeToPaxLogFile(int counter, String type, ElementsSetProto acceptedValue){

		StringBuffer buffer = new StringBuffer();
		buffer.append(counter+":"+type+"=");
		for(ElementProto elem: acceptedValue.getElementsList())
		{
			buffer.append(elem.getRow()+":");
			for(ColElementProto col:     elem.getColsList())
			{
				buffer.append(col.getCol()+","+col.getValue());
				buffer.append(";");
			}
			buffer.append(":,:");
		}
		PAXOSLOG.info(buffer.toString());

	}

	private int getLastPaxosInstanceNumber()
	{
		int count;
		try {
			count = logRAF.readInt();
			return count;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
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

	/*public void readStreamMessages() throws NumberFormatException, IOException
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter Paxos Instance ID ");
		String uid = br.readLine();
		System.out.println("Enter value to initiate Paxos");
		int value = Integer.parseInt(br.readLine());
		sendPrepareMessage(uid, value);
	}*/


	public void checkForAbortedTrans() 
	{
		while(true)
		{
			Long curTime = new Date().getTime();
			for(String uid: pendingPaxosInstances)
			{
				PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
				if(curTime - paxInstance.getTimeStamp() > Common.TRANS_TIMEOUT)
				{
					System.out.println("Transaction timed out. Sending Abort message ");

					pendingPaxosInstances.remove(uid);

					ClientOpMsg message = new ClientOpMsg(nodeAddress, uidTransMap.get(uid).getTrans(), ClientOPMsgType.ABORT);
					SendClientMessage(message, uidTransMap.get(uid).getSource());
					//send abort msg to all participants too: fix me


				}
				else{
					System.out.println("Sending DECIDE msg to all after 2 secs ");
					PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
					sendDecideMsg(message);
				}

			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void run()
	{
		while (!Thread.currentThread().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			//System.out.println("Received Messsage &&&&&&&&&&&&&& "+receivedMsg);

			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);
			//	System.out.println("Msg wrap updated -------------------------------- ");
			if (msgwrap != null ) {

				try {
					if(msgwrap.getmessageclass() == ClientOpMsg.class && this.state == PLeaderState.ACTIVE)
					{

						ClientOpMsg msg = (ClientOpMsg) msgwrap.getDeSerializedInnerMessage();
						if(msg.getMsgType() == ClientOPMsgType.READ)
						{
							handleClientReadMessage(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.WRITE)
						{
							System.out.println("Client Write msg >>>>>>>>>>>>>>> ");
							handleClientWriteMessage(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.UNLOCK)
						{
							handleClientUnLockMsg(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.RELEASE_RESOURCE)
						{
							handleClientReleaseResourceMsg(msg);
						}

					}

					if(msgwrap.getmessageclass() == PaxosMsg.class && state == PLeaderState.ACTIVE)
					{
						PaxosMsg msg = (PaxosMsg)msgwrap.getDeSerializedInnerMessage();
						if(msg.getType() == PaxosMsgType.PREPARE)
						{
							handlePaxosPrepareMessage(msg);
						}
						if(msg.getType() == PaxosMsgType.ACK)
						{
							handlePaxosAckMessage(msg);
						}
						if(msg.getType() == PaxosMsgType.ACCEPT)
						{
							handlePaxosAcceptMessage(msg);
						}
						if(msg.getType() == PaxosMsgType.DECIDE)
						{
							handlePaxosDecideMessage(msg);
						}
					}
					if(msgwrap.getmessageclass() == PaxosDetailsMsg.class )
					{
						PaxosDetailsMsg msg = (PaxosDetailsMsg)msgwrap.getDeSerializedInnerMessage();
						handlePaxosDetailsMsg(msg);
					}

					
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
								handleCommitMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.ABORT){
							if(msg.isTwoPC())
								twoPhaseCoordinator.ProcessAbortMessage(msg);
							else
								handleAbortMessage(msg);
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


	private void handleClientReadMessage(ClientOpMsg message) throws IOException
	{

		System.out.println("Inside process client read msg");
		NodeProto transClient = message.getSource();
		TransactionProto trans = message.getTransaction();
		System.out.println("Read set ::::::: "+trans.getReadSet());

		boolean isReadLock = true;
		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			if(!lockTable.acquireLock(element.getRow(), trans.getTransactionID()))
				isReadLock = false;
		}

		System.out.println("Acquired all read locks."+isReadLock);
		printLocks();
		ClientOpMsg read_response = null;
		if(isReadLock){
			ElementsSetProto readValues = localResource.ReadResource(trans.getReadSet());
			writeToPaxLogFile(++logCounter, "READ", readValues);

			TransactionProto transaction = TransactionProto.newBuilder()
					.setTransactionID(trans.getTransactionID())
					.setTransactionStatus(TransactionStatusProto.ACTIVE)
					.setReadSet(readValues)
					.build();

			System.out.println("Preparing Client Read Response");


			read_response = new ClientOpMsg(nodeAddress, transaction, ClientOPMsgType.READ_RESPONSE, true);
		}
		else
			read_response = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.READ_RESPONSE, false);

		SendClientMessage(read_response, transClient);
		System.out.println("Sent Read Data for UID - "+trans.getTransactionID());		
		LOGGER.log(Level.FINE, new String("Sent Read Data for UID - "+trans.getTransactionID()));

	}

	private void handleClientUnLockMsg(ClientOpMsg msg) throws IOException
	{
		TransactionProto trans = msg.getTransaction();
		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			lockTable.releaseLock(element.getRow(), trans.getTransactionID());
		}
		System.out.println("Released all locks for trans "+msg.getTransaction().getTransactionID());
		printLocks();
	}

	private void printLocks()
	{
		lockTable.printLocks();
	}

	private void handleClientReleaseResourceMsg(ClientOpMsg message)
	{
		System.out.println("Inside process client release resource msg");
		TransactionProto trans = message.getTransaction();

		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			lockTable.releaseLock(element.getRow(), trans.getTransactionID());
		}
		System.out.println("Released all resources. No ack sent");

	}

	private void releaseLocks(ElementsSetProto elementsSetProto, String uid)
	{
		for(ElementProto element : elementsSetProto.getElementsList())
		{
			lockTable.releaseLock(element.getRow(), uid);
		}
		System.out.println("Released all resources. No ack sent");

	}

	private void handleClientWriteMessage(ClientOpMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();
			
			System.out.println("New write trans from ---------- "+msg.getSource());
			uidTransMap.put(trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));
			boolean isWriteLock = true;
			for(ElementProto element : trans.getWriteSet().getElementsList())
			{
				if(!lockTable.acquireLock(element.getRow(), trans.getTransactionID()))
				{
					isWriteLock = false;
				}
				else
					lockTable.acquireReadLockIfNot(element.getRow(), trans.getTransactionID());

			}
			System.out.println("IsWritelock Acquired "+isWriteLock);
			if(isWriteLock){
				//Already leader. Send Accept right Away
				int newLogPosition = logCounter++;
				logLocationtoTransIdMap.put(newLogPosition, new LogPositionWritten(trans.getTransactionID()));

				PaxosInstance paxInstance = new PaxosInstance(trans.getTransactionID(), ballotNo, trans.getWriteSet());

				uidPaxosInstanceMap.put(trans.getTransactionID(), paxInstance);
				PaxosMsg message = new PaxosMsg(nodeAddress, trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				pendingPaxosInstances.add(trans.getTransactionID());
				paxInstance.addtoAcceptList(nodeAddress);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			}
			else{
				System.out.println("Not able to acquire locks. Aborting the trans");
				//ClientOpMsg message = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.ABORT);
				//SendClientMessage(message, msg.getSource());
				TwoPCMsg message = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
		else{
			//yet to fill in
		}
	}
	
	
	
	private void handleCommitMessage(TwoPCMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();
			
			System.out.println("New commit trans from ---------- "+msg.getSource());
			uidTransMap.put(trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.COMMIT));
			
				//Already leader. Send Accept right Away
				int newLogPosition = logCounter++;
				logLocationtoTransIdMap.put(newLogPosition, new LogPositionWritten(trans.getTransactionID()));

				PaxosInstance paxInstance = new PaxosInstance(trans.getTransactionID(), ballotNo, trans.getWriteSet());

				uidPaxosInstanceMap.put(trans.getTransactionID(), paxInstance);
				PaxosMsg message = new PaxosMsg(nodeAddress, trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				pendingPaxosInstances.add(trans.getTransactionID());
				paxInstance.addtoAcceptList(nodeAddress);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			
		}
		else{
			//yet to fill in
		}
	}
	
	
	private void handleAbortMessage(TwoPCMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();
			
			System.out.println("New commit trans from ---------- "+msg.getSource());
			uidTransMap.put(trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.ABORT));
			
				//Already leader. Send Accept right Away
				int newLogPosition = logCounter++;
				logLocationtoTransIdMap.put(newLogPosition, new LogPositionWritten(trans.getTransactionID()));

				PaxosInstance paxInstance = new PaxosInstance(trans.getTransactionID(), ballotNo, trans.getWriteSet());

				uidPaxosInstanceMap.put(trans.getTransactionID(), paxInstance);
				PaxosMsg message = new PaxosMsg(nodeAddress, trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				pendingPaxosInstances.add(trans.getTransactionID());
				paxInstance.addtoAcceptList(nodeAddress);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			
		}
		else{
			//yet to fill in
		}
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
	
	private void handlePaxosDecideMessage(PaxosMsg msg)
	{
		System.out.println("Handling Decide msg from "+msg.getSource());
		if(pendingPaxosInstances.contains(msg.getUID())){
			System.out.println("Pending paxos instance ");
			if(uidPaxosInstanceMap.containsKey(msg.getUID()))
			{
				PaxosInstance paxInstance = uidPaxosInstanceMap.get(msg.getUID());
				paxInstance.addToDecideList(nodeAddress);
				System.out.println("Decide list count "+paxInstance.getDecidesCount());
				System.out.println(" "+paxInstance.decides);
				if( !paxInstance.isCommited)
				{
					paxInstance.isDecideSent = true;
					paxInstance.addToDecideList(msg.getSource());
					paxInstance.addToDecideList(nodeAddress);
					System.out.println("AcceptValue decided to be "+paxInstance.getAcceptedValue());
					paxInstance.setCommited();
					Boolean isWritten = localResource.WriteResource(paxInstance.getAcceptedValue());
					if(isWritten){
						writeToPaxLogFile(++logCounter, "WRITE", paxInstance.getAcceptedValue());
						releaseLocks(paxInstance.getAcceptedValue(), msg.getUID());

						if(isLeader)
						{
							//send response to TwoPC
							TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
							if(tempTransSource.getType() == TwoPCMsgType.PREPARE)
							{
								System.out.println("Sending PrepareAck to TwoPC "+tempTransSource.getSource());
								//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
								//SendClientMessage(message, tempTransSource.getSource());
								TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(msg.getUID()).getTrans(), TwoPCMsgType.PREPARE);
							
								SendTwoPCMessage(message, uidTransMap.get(msg.getUID()).getSource());
								System.out.println("Sending Prepare Ack for UID - "+msg.getUID());		
								LOGGER.log(Level.FINE, new String("Sent Prepare Ack for UID - "+msg.getUID()));
							}
							else if(tempTransSource.getType() == TwoPCMsgType.COMMIT){
							System.out.println("Sending Commit Ack to "+tempTransSource.getSource());
							//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
							//SendClientMessage(message, tempTransSource.getSource());
							TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(msg.getUID()).getTrans(), TwoPCMsgType.COMMIT);
						
							SendTwoPCMessage(message, uidTransMap.get(msg.getUID()).getSource());
							System.out.println("Sending Commit Ack for UID - "+msg.getUID());		
							LOGGER.log(Level.FINE, new String("Sent Commit Ack for UID - "+msg.getUID()));
							}
							
							else if(tempTransSource.getType() == TwoPCMsgType.ABORT){
								System.out.println("Sending Abort Ack to "+tempTransSource.getSource());
								//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
								//SendClientMessage(message, tempTransSource.getSource());
								TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(msg.getUID()).getTrans(), TwoPCMsgType.ABORT);
							
								SendTwoPCMessage(message, uidTransMap.get(msg.getUID()).getSource());
								System.out.println("Sending Abort Ack for UID - "+msg.getUID());		
								LOGGER.log(Level.FINE, new String("Sent Abort Ack for UID - "+msg.getUID()));
								}
							
						}

						PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(), PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
						sendDecideMsg(message);
					}
					else{
						throw new IllegalArgumentException("Write failed ");
					}
				}
				else{
					//Paxos Instance already commited. No action to be taken
					System.out.println("Pax instance already committed ::: ");
					boolean isDone = false;
					if( paxInstance.getDecidesCount() == acceptorsCount) 
						isDone = true;
					paxInstance.addToDecideList(msg.getSource());
					System.out.println("No of decide count :: "+paxInstance.getDecidesCount()+"  acceptors count "+acceptorsCount);
					/*if(paxInstance.getDecidesCount() < acceptorsCount )
					{
						PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(), PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
						sendDecideMsg(message);
					}*/
					if( isDone) {
						//every node has decided, remove from the pending list
						System.out.println("Stopped sending DECIDE Messsage as all participants have sent DECIDE msg");
					
						pendingPaxosInstances.remove(msg.getUID());
					}
				}
				uidPaxosInstanceMap.put(msg.getUID(), paxInstance);
			}
			else{
				System.out.println("Paxos Instance not found. Creating new instance ");

				PaxosInstance paxInstance = new PaxosInstance(msg.getUID(), msg.getBallotNumber(), msg.getAcceptValue());
				//not sending any accept msg. Directly sending decide message
				paxInstance.setAcceptSent();
				paxInstance.isDecideSent = true;
				paxInstance.setTimeStamp(new Date().getTime());
				for(ElementProto elementProto : msg.getAcceptValue().getElementsList())
				{
					lockTable.acquireLock(elementProto.getRow(), msg.getUID());
				}

				if(paxInstance.getDecidesCount() < acceptorsCount )
				{
					paxInstance.addToDecideList(nodeAddress);
					PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(), PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
					sendDecideMsg(message);
					uidPaxosInstanceMap.put(msg.getUID(), paxInstance);
				}
				else{
					//every node has decided, remove from the pending list
					pendingPaxosInstances.remove(msg.getUID());
					uidTransMap.remove(msg.getUID());
					uidPaxosInstanceMap.remove(msg.getUID());
				}

			}
		}
		else{
			

			if(uidPaxosInstanceMap.containsKey(msg.getUID())){
				pendingPaxosInstances.add(msg.getUID());
				PaxosInstance paxInstance = uidPaxosInstanceMap.get(msg.getUID());
				
				if(!paxInstance.isDecideSent && !paxInstance.isCommited()){
					paxInstance.setAcceptSent();
					paxInstance.setTimeStamp(new Date().getTime());

					paxInstance.isDecideSent = true;
					paxInstance.addToDecideList(msg.getSource());

					paxInstance.addToDecideList(nodeAddress);

					for(ElementProto elementProto : msg.getAcceptValue().getElementsList())
					{
						lockTable.acquireLock(elementProto.getRow(), msg.getUID());
					}

					PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(), PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
					sendDecideMsg(message);
					uidPaxosInstanceMap.put(msg.getUID(), paxInstance);
				}
			}
			else{
				System.out.println("Already decided on the value for this pax instance. So ignoring safely");
			}

		}
	}
	
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

	private void handlePaxosAcceptMessage(PaxosMsg msg)
	{
		System.out.println("Received Accept msg "+msg);
		if(logLocationtoTransIdMap.containsKey(msg.getLogPositionNumber())){
			if(uidPaxosInstanceMap.containsKey(msg.getUID()))
			{
				PaxosInstance paxInstance = uidPaxosInstanceMap.get(msg.getUID());
				int comparedValue = msg.getBallotNumber().compareTo(paxInstance.getBallotNumber());
				System.out.println("Compared value :::: "+comparedValue);
				if(comparedValue >1 || comparedValue == 0)
				{
					paxInstance.setAcceptNumber(msg.getBallotNumber());
					paxInstance.setAcceptedValue(msg.getAcceptValue());
					/*if(paxInstance.getAcceptCount() == 0)
					{
						//unreachable code
						System.out.println("Unreachable Code &&&&&&&&& ");
						paxInstance.addtoAcceptList(msg.getSource());
						//first time. send accept to all
						System.out.println("Frist time receiving ACCEPT, sending accept to all");
						System.out.println("List of accepted nodes ");
						System.out.println(" "+paxInstance.getAcceptList());
						PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(),PaxosMsgType.ACCEPT, msg.getBallotNumber(), msg.getAcceptValue());
						sendAcceptMsg(message);
					}
					else*/
					if( !paxInstance.isAcceptSent())
					{
						//original msg from leader is lost or due to whatever reason. Getting accept msg from another acceptor
						String uid = msg.getUID();
						logLocationtoTransIdMap.put(msg.getLogPositionNumber(), new LogPositionWritten(uid));

						paxInstance.addtoAcceptList(nodeAddress);
						paxInstance.setAcceptSent();
						uidPaxosInstanceMap.put(uid, paxInstance );
						PaxosMsg message = new PaxosMsg(nodeAddress, uid,PaxosMsgType.ACCEPT, msg.getBallotNumber(),msg.getAcceptValue());
						message.setLogPositionNumber(msg.getLogPositionNumber());

						sendAcceptMsg( message );
					}
					else{
						System.out.println("Already sent ACCEPT, not forwarding to others ");
						// already sent accept msg. dont take any action
						paxInstance.addtoAcceptList(msg.getSource());
						System.out.println("Accept Count "+paxInstance.getAcceptCount());
						System.out.println("List of accepted nodes ");
						System.out.println(" "+paxInstance.getAcceptList());
					}

					if( !paxInstance.isDecideSent)
					{
						if(paxInstance.getAcceptCount() > this.acceptors.size()/2)
						{
							System.out.println("Accept count (after majority) "+paxInstance.getAcceptCount());
							System.out.println("Is Decide sent "+paxInstance.isDecideSent);

							pendingPaxosInstances.add(msg.getUID());
							paxInstance.setTimeStamp(new Date().getTime());
							paxInstance.isDecideSent = true;
							paxInstance.addToDecideList(nodeAddress);
							System.out.println("AcceptValue decided to be "+paxInstance.getAcceptedValue());
							paxInstance.setCommited();
							writeToPaxLogFile(++logCounter, "WRITE", paxInstance.getAcceptedValue());
							printLocks();
							Boolean isWritten = localResource.WriteResource(paxInstance.getAcceptedValue());

							if(isWritten){
								if(isLeader)
								{
									//send response to TPC
									TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
									
									if(tempTransSource.getType() == TwoPCMsgType.PREPARE)
									{
										System.out.println("Sending PrepareAck to TwoPC "+tempTransSource.getSource());
										//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
										//SendClientMessage(message, tempTransSource.getSource());
										//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
										//SendClientMessage(message, tempTransSource.getSource());
										
										TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(msg.getUID()).getTrans(), TwoPCMsgType.PREPARE);
									
										SendTwoPCMessage(message, uidTransMap.get(msg.getUID()).getSource());
										System.out.println("Sending Prepare Ack for UID - "+msg.getUID());		
										LOGGER.log(Level.FINE, new String("Sent Prepare Ack for UID - "+msg.getUID()));
									}
									else if(tempTransSource.getType() == TwoPCMsgType.COMMIT){
									System.out.println("Sending Commit Ack to "+tempTransSource.getSource());
									//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
									//SendClientMessage(message, tempTransSource.getSource());
									TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(msg.getUID()).getTrans(), TwoPCMsgType.COMMIT);
								
									SendTwoPCMessage(message, uidTransMap.get(msg.getUID()).getSource());
									System.out.println("Sending Commit Ack for UID - "+msg.getUID());		
									LOGGER.log(Level.FINE, new String("Sent Commit Ack for UID - "+msg.getUID()));
									}
									
									else if(tempTransSource.getType() == TwoPCMsgType.ABORT){
										System.out.println("Sending Abort Ack to "+tempTransSource.getSource());
										//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
										//SendClientMessage(message, tempTransSource.getSource());
										TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(msg.getUID()).getTrans(), TwoPCMsgType.ABORT);
									
										SendTwoPCMessage(message, uidTransMap.get(msg.getUID()).getSource());
										System.out.println("Sending Abort Ack for UID - "+msg.getUID());		
										LOGGER.log(Level.FINE, new String("Sent Abort Ack for UID - "+msg.getUID()));
										}
									
									/*
									System.out.println("Sending client response(COMMIT) to "+tempTransSource.getSource());
									ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
									SendClientMessage(message, tempTransSource.getSource());*/
								}

								releaseLocks(paxInstance.getAcceptedValue(), msg.getUID());
								PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(), PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
								sendDecideMsg(message);
							}
							else{
								//send client response
								TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
								System.out.println("Sending client response(ABORT) to "+tempTransSource.getSource());
								ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
								SendClientMessage(message, tempTransSource.getSource());
							}
						}
					}

				}
				uidPaxosInstanceMap.put(msg.getUID(), paxInstance);
			}

			else{
				System.out.println("Not Reachable Code. ");
				throw new IllegalStateException("Receiving Ack for non-existent PaxosInstance");
			}
		}
		else{
			System.out.println("Leader fixed and sends accept msg. Accepter receiving accept msg for the first time for this log position");
			String uid = msg.getUID();
			logLocationtoTransIdMap.put(msg.getLogPositionNumber(), new LogPositionWritten(uid));
			PaxosInstance paxInstance = new PaxosInstance(uid, msg.getBallotNumber(), msg.getAcceptValue());
			paxInstance.addtoAcceptList(msg.getSource());
			paxInstance.addtoAcceptList(nodeAddress);
			boolean isWriteLock = true;
			for(ElementProto element : paxInstance.getAcceptedValue().getElementsList())
			{
				if(!lockTable.acquireLock(element.getRow(), uid))
				{
					isWriteLock = false;
				}

			}

			if(isWriteLock){
				paxInstance.setAcceptSent();
				uidPaxosInstanceMap.put(uid, paxInstance );
				PaxosMsg message = new PaxosMsg(nodeAddress, uid,PaxosMsgType.ACCEPT, msg.getBallotNumber(),msg.getAcceptValue());
				message.setLogPositionNumber(msg.getLogPositionNumber());

				sendAcceptMsg( message );
			}
		}

	}

	private void sendDecideMsg(PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				sendPaxosMsg(node, msg);
			}
		}
	}

	private void handlePaxosAckMessage(PaxosMsg msg)
	{
		System.out.println("Received Ack Msg "+msg);
		if(uidPaxosInstanceMap.containsKey(msg.getUID()))
		{
			PaxosInstance paxInstance = uidPaxosInstanceMap.get(msg.getUID());
			paxInstance.addAcceptorToAck(msg.getSource());
			if(paxInstance.getHighestAcceptNo() == null || paxInstance.getHighestAcceptNo().compareTo(msg.getAcceptNo()) < 0 )
			{
				paxInstance.setHighestAcceptNo(msg.getAcceptNo());
				paxInstance.setAcceptedValue(msg.getAcceptValue());
			}
			if(paxInstance.getAckCount() > this.acceptors.size()/2)
			{
				if(!paxInstance.isAcceptSent())
				{
					System.out.println(nodeId+" chosen as the leader ************ ");
					ElementsSetProto acceptVal = paxInstance.getAcceptedValue();
					PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), acceptVal);
					paxInstance.setAcceptSent();
					paxInstance.addtoAcceptList(nodeAddress);

					sendAcceptMsg(message);
				}
				else{
					//accept msg already sent. nothing to do
				}
			}
		}
		else{
			System.out.println("Not Reachable Code. ");
			throw new IllegalStateException("Receiving Ack for non-existent PaxosInstance");
		}
	}

	private void sendAcceptMsg(PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				sendPaxosMsg(node, msg);
			}
		}
	}

	private void sendAcceptMsgExcept(NodeProto remove,PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress) && !node.equals(remove))
			{
				sendPaxosMsg(node, msg);
			}
		}
	}


	private void appendToLogFile(PaxosInstance paxosInstance)
	{

	}


	private void handlePaxosPrepareMessage(PaxosMsg msg)
	{
		System.out.println("Handling Prepare msg "+msg);
		String tempUID = msg.getUID();
		if(uidPaxosInstanceMap.containsKey(tempUID))
		{
			PaxosInstance paxInstance = uidPaxosInstanceMap.get(tempUID);
			int comparedValue = paxInstance.getBallotNumber().compareTo(msg.getBallotNumber());
			if(comparedValue > 0 || comparedValue == 0)
			{
				paxInstance.setBallotNumber(msg.getBallotNumber());
				PaxosMsg message = new PaxosMsg(nodeAddress, tempUID, PaxosMsgType.ACK ,msg.getBallotNumber(), paxInstance.getAcceptedNumber(), paxInstance.getAcceptedValue());
				sendPaxosMsg(msg.getSource(), message);
			}
			else{
				//Don't send ack msg
			}
		}
		else{
			System.out.println("handling Prepare msg for UID "+tempUID);
			PaxosInstance paxInstance = new PaxosInstance(tempUID, msg.getBallotNumber());
			paxInstance.setAcceptNumber(msg.getBallotNumber());
			uidPaxosInstanceMap.put(tempUID, paxInstance);
			PaxosMsg message = new PaxosMsg(nodeAddress, tempUID, PaxosMsgType.ACK ,msg.getBallotNumber(), paxInstance.getAcceptedNumber(), paxInstance.getAcceptedValue());
			sendPaxosMsg(msg.getSource(), message);
		}
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
		acceptorsCount = acceptors.size();
		System.out.println("List of Acceptors "+acceptors);
		this.state = PLeaderState.ACTIVE;
		//	initPaxosInstance();
	}

	private void initPaxosInstance()
	{
		//sendPrepareMessage();
		System.out.println("Received all acceptors list. Ready to initiate Paxos");
	}

	/*private void sendPrepareMessage(String uid, int value)
	{
		ballotNo = new BallotNumber(ballotNo.getBallotNo()+1, myId);
		this.acceptedValue = value;
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo);
		paxInstance.addAcceptorToAck(nodeAddress);
		paxInstance.setHighestAcceptNo(ballotNo);
		paxInstance.setAcceptedValue(value);
		uidPaxosInstanceMap.put(uid, paxInstance);
		System.out.println("List of UIDs in uidPaxosInstanceMap :: ");
		for(String str: uidPaxosInstanceMap.keySet())
			System.out.print(" "+str);
		System.out.println();
		PaxosMsg msg = new PaxosMsg(nodeAddress,uid, PaxosMsgType.PREPARE, ballotNo);
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				sendPaxosMsg(node, msg);
			}
		}
	}*/

	private void sendPrepareMessage()
	{
		ballotNo = new BallotNumber(ballotNo.getBallotNo()+1, myId);
		String uid = java.util.UUID.randomUUID().toString();
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo);
		paxInstance.addAcceptorToAck(nodeAddress);
		uidPaxosInstanceMap.put(uid, paxInstance);
		PaxosMsg msg = new PaxosMsg(nodeAddress,uid, PaxosMsgType.PREPARE, ballotNo);
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				sendPaxosMsg(node, msg);
			}
		}

	}

	private void sendPaxosMsg(NodeProto dest, PaxosMsg msg){
		System.out.println("Sent " + msg+"\n from "+nodeAddress +" to "+dest.getHost()+":"+dest.getPort());
		this.AddLogEntry("Sent "+msg, Level.INFO);

		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();	
	}

	public static void main(String args[]) throws IOException
	{
		if(args.length <= 1)
			throw new IllegalArgumentException("Usage: PAcceptor <ShardID> <nodeId>");
		PAcceptor_Mod acceptor = new PAcceptor_Mod(args[0], args[1]);
		new Thread(acceptor).start();
		//acceptor.readStreamMessages();
		acceptor.checkForAbortedTrans();
	}


}
