package spanner.node;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.ResourceHM;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PLeaderState;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.TwoPCMsgType;
import spanner.locks.LockTable;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosDetailsMsg;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

public class PaxosAcceptor extends Node implements Runnable{

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
	LockTable lockTable = null;
	private static Logger PAXOSLOG = null;
	int acceptorsCount = 0;
	BallotNumber ballotNo = null;
	private HashMap<Integer, LogPositionWritten> logLocationtoTransIdMap = null; 
	private HashMap<String, PaxosInstance> uidPaxosInstanceMap = null;
	private HashSet<String> pendingPaxosInstances = null;
	private HashMap<String, TransactionSource> uidTransMap = null;
	TwoPC twoPhaseCoordinator = null;
	private int logCounter = 0;
	RandomAccessFile logRAF = null;
	private static ResourceHM localResource = null;

	public PaxosAcceptor(String shard, String nodeId) throws IOException
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
		
		//create Log file
		createLogFile(shard, nodeId);

		//socket.connect(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));

		//socket.bind(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket.bind("tcp://127.0.0.1:"+hostDetails[1]);
		InetAddress addr = InetAddress.getLocalHost();
		nodeAddress = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(Integer.parseInt(hostDetails[1])).build();
		twoPhaseCoordinator = new TwoPC(nodeAddress, context);

		System.out.println("Participant node address ****** "+nodeAddress);
		String[] mds = Common.getProperty("mds").split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		lockTable = new LockTable();
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


	public void checkForPendingTrans() 
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


					//send abort msg to all participants too: fix me
					paxInstance.isCommited();
					uidPaxosInstanceMap.put(uid, paxInstance);

					/*ClientOpMsg message = new ClientOpMsg(nodeAddress, uidTransMap.get(uid).getTrans(), ClientOPMsgType.ABORT);
					SendClientMessage(message, uidTransMap.get(uid).getSource());*/
					TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(uid).getTrans(), TwoPCMsgType.ABORT);
					SendTwoPCMessage(message, uidTransMap.get(uid).getSource());

				}
				else{

					if(paxInstance.decides.size() != acceptorsCount){
						System.out.println("Sending DECIDE msg to all after 2 secs ");
						System.out.println(" "+paxInstance.decides);
						System.out.println("List of decides ^^^^^^^^^^^^^^^ ");
						PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
						sendDecideMsg(message);
					}
					else{
						System.out.println("Daemon thread found that all acceptors have DECIDED. Hence removing from pending list::::::: ");
						if(!paxInstance.isDecideComplete)
						{
							PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
							sendDecideMsg(message);
							paxInstance.isDecideComplete = true;
							uidPaxosInstanceMap.put(uid, paxInstance);
						}
						pendingPaxosInstances.remove(uid);
					}
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


	private void sendPaxosMsg(NodeProto dest, PaxosMsg msg){
		System.out.println("Sent " + msg+"\n from "+nodeAddress +" to "+dest.getHost()+":"+dest.getPort());
		this.AddLogEntry("Sent "+msg, Level.INFO);

		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();	
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
							//handlePaxosPrepareMessage(msg);
						}
						if(msg.getType() == PaxosMsgType.ACK)
						{
							//	handlePaxosAckMessage(msg);
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
							twoPhaseCoordinator.ProcessInfoMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.PREPARE)
						{	
							twoPhaseCoordinator.ProcessPrepareMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.COMMIT){
							if(msg.isTwoPC())
								twoPhaseCoordinator.ProcessCommitMessage(msg);
							else
								handleTwoPCCommitMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.ABORT){
							if(msg.isTwoPC())
								twoPhaseCoordinator.ProcessAbortMessage(msg);
							else
								handleTwoPCAbortMessage(msg);
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

		System.out.println("Handling process client read msg");
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

	/*	
	private void handleClientUnLockMsg(ClientOpMsg msg) throws IOException
	{
		TransactionProto trans = msg.getTransaction();
		for(ElementProto element : trans.getReadSet().getElementsList())
		{
			lockTable.releaseLock(element.getRow(), trans.getTransactionID());
		}
		System.out.println("Released all locks for trans "+msg.getTransaction().getTransactionID());
		printLocks();
	}*/

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

	private void handleClientWriteMessage(ClientOpMsg msg)
	{
		//System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();

			System.out.println("New write trans from ---------- "+msg.getSource());
			//uidTransMap.put("P"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));
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

				PaxosInstance paxInstance = new PaxosInstance("P"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				//paxInstanceToUIDMap.put("P"+trans.getTransactionID(), trans.getTransactionID());
				uidPaxosInstanceMap.put("P"+trans.getTransactionID(), paxInstance);
				uidTransMap.put( "P"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.PREPARE));

				PaxosMsg message = new PaxosMsg(nodeAddress, "P"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				//pendingPaxosInstances.add("P"+trans.getTransactionID());
				paxInstance.addtoAcceptList(nodeAddress);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			}
			else{
				System.out.println("Not able to acquire locks. Aborting the trans");

				//ClientOpMsg message = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.ABORT);
				//SendClientMessage(message, msg.getSource());
				//Fix me
				TwoPCMsg message = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
		else{
			//yet to fill in
		}
	}


	private void handleTwoPCCommitMessage(TwoPCMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();

			System.out.println("New commit trans from ---------- "+msg.getSource());
			//uidTransMap.put("P"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));
			//FIX ME: is the below block required
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

				PaxosInstance paxInstance = new PaxosInstance("C"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				//paxInstanceToUIDMap.put("P"+trans.getTransactionID(), trans.getTransactionID());
				uidPaxosInstanceMap.put("C"+trans.getTransactionID(), paxInstance);
				uidTransMap.put( "C"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.PREPARE));

				PaxosMsg message = new PaxosMsg(nodeAddress, "C"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				//pendingPaxosInstances.add("C"+trans.getTransactionID());
				paxInstance.addtoAcceptList(nodeAddress);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			}
			else{
				System.out.println("Not able to acquire locks. Aborting the trans");

				//ClientOpMsg message = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.ABORT);
				//SendClientMessage(message, msg.getSource());
				//Fix me
				TwoPCMsg message = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
		else{
			//yet to fill in
		}
	}


	private void handleTwoPCAbortMessage(TwoPCMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();

			System.out.println("New Abort trans from ---------- "+msg.getSource());
			//uidTransMap.put("P"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));

			//Already leader. Send Accept right Away
			int newLogPosition = logCounter++;
			logLocationtoTransIdMap.put(newLogPosition, new LogPositionWritten(trans.getTransactionID()));

			PaxosInstance paxInstance = new PaxosInstance("A"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
			//paxInstanceToUIDMap.put("P"+trans.getTransactionID(), trans.getTransactionID());
			uidPaxosInstanceMap.put("A"+trans.getTransactionID(), paxInstance);
			uidTransMap.put( "A"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.PREPARE));

			PaxosMsg message = new PaxosMsg(nodeAddress, "A"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
			message.setLogPositionNumber(newLogPosition);
			paxInstance.setAcceptSent();
			paxInstance.setTimeStamp(new Date().getTime());
			//pendingPaxosInstances.add("A"+trans.getTransactionID());
			paxInstance.addtoAcceptList(nodeAddress);
			System.out.println("Sending accept msgs to all acceptors ");
			sendAcceptMsg(message);
		}
		else{
			//yet to fill in
		}
	}

	private void handlePaxosDecideMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		if(uidPaxosInstanceMap.containsKey(uid))
		{
			//System.out.println("Handling decide msg inside 1st if loop");
			//System.out.println("Already ACCEPT sent to all. Adding to the list of acceptors");
			PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
			int prevCount = paxInstance.decides.size();
			paxInstance.decides.add(msg.getSource());
			int newCount = paxInstance.decides.size();
			if(prevCount < newCount ){
				if(newCount >  acceptorsCount/2)
				{
					if(!paxInstance.isDecideSent)
					{
						System.out.println("Reached majority of DECIDE. DECIDING and sending DECIDE msg");

						pendingPaxosInstances.add(msg.getUID());
						paxInstance.setTimeStamp(new Date().getTime());
						paxInstance.isDecideSent = true;
						paxInstance.addToDecideList(nodeAddress);
						System.out.println("AcceptValue decided to be "+paxInstance.getAcceptedValue());
						paxInstance.setCommited();
						writeToPaxLogFile(++logCounter, "WRITE", paxInstance.getAcceptedValue());
						//	printLocks();
						if(uid.startsWith("C")){
							Boolean isWritten = localResource.WriteResource(paxInstance.getAcceptedValue());

							if(isWritten){
								if(isLeader)
								{
									//send response for the Paxos Instance
									System.out.println("Decided on a value(Commited), sent response to TPC ");
									sendPaxosInstanceResponse(msg);

								}
								System.out.println("Releasing the resources");
								releaseLocks(paxInstance.getAcceptedValue(), msg.getUID().substring(1));
							}
							else{
								//send client response
								///FIX me . send to TPC
								TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
								System.out.println("Sending client response(ABORT) to "+tempTransSource.getSource());
								//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
								//SendClientMessage(message, tempTransSource.getSource());
								TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT);
								SendTwoPCMessage(message, msg.getSource());
							}
						}
						else if(uid.startsWith("P")){//Pax instance for PREPARE phase. Just obtained read locks
							System.out.println("Done with PREPARE phase, sent response to TPC ");
							sendPaxosInstanceResponse(msg);

						}
						else if(uid.startsWith("A"))
						{
							System.out.println("Done with ABORT, sent response to TPC ");
							sendPaxosInstanceResponse(msg);
						}


					}
					else{
						System.out.println("Reached majority and already DECIDED. No action taken");

					}

					if(newCount == acceptorsCount)
					{
						System.out.println("All acceptors decided. Stopping DECIDE broadcast");
						if(!paxInstance.isDecideComplete)
						{
							PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, paxInstance.getAcceptedValue());
							sendDecideMsg(message);
							paxInstance.isDecideComplete = true;
						}
						pendingPaxosInstances.remove(uid);
					}
				}

			}
			else{
				if(!paxInstance.isDecideSent)
				{
					pendingPaxosInstances.add(msg.getUID());
					paxInstance.setTimeStamp(new Date().getTime());
					paxInstance.isDecideSent = true;
					paxInstance.addToDecideList(nodeAddress);
				}
			}
			uidPaxosInstanceMap.put(uid, paxInstance);
		}
		else{
			System.out.println("First time seeing DECIDE msg. DECIDING the value and sending DECIDE to all");
			PaxosInstance paxInstance  = null;
			if(uidPaxosInstanceMap.containsKey(uid)){
				paxInstance = uidPaxosInstanceMap.get(uid);
			}
			else{
				paxInstance = new PaxosInstance(uid, msg.getBallotNumber(), msg.getAcceptNo(), msg.getAcceptValue());
			}

			paxInstance.addToDecideList(msg.getSource());
			paxInstance.addToDecideList(nodeAddress);
			uidPaxosInstanceMap.put(uid, paxInstance );
			PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
			message.setLogPositionNumber(msg.getLogPositionNumber());
			sendDecideMsg(message);
		}


	}

	private void handlePaxosAcceptMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		if(uidPaxosInstanceMap.containsKey(uid))
		{
			System.out.println("Already ACCEPT sent to all. Adding to the list of acceptors");
			PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
			int prevCount = paxInstance.accepts.size();
			paxInstance.accepts.add(msg.getSource());
			int newCount = paxInstance.accepts.size();
			if(prevCount < newCount ){
				if(newCount >  acceptorsCount/2)
				{
					if(!paxInstance.isDecideSent)
					{
						System.out.println("Reached majority of acceptors, but DECIDE not yet sent. DECIDING and sending DECIDE msg");

						pendingPaxosInstances.add(msg.getUID());
						paxInstance.setTimeStamp(new Date().getTime());
						paxInstance.isDecideSent = true;
						paxInstance.addToDecideList(nodeAddress);
						System.out.println("AcceptValue decided to be "+paxInstance.getAcceptedValue());
						paxInstance.setCommited();
						//Fix me: either of PREPARE, COMMIT or ABORT
						writeToPaxLogFile(++logCounter, "WRITE", paxInstance.getAcceptedValue());
						//	printLocks();
						//Fix me: only incase of COMMIT
						Boolean isWritten = localResource.WriteResource(paxInstance.getAcceptedValue());

						if(isWritten){
							//send DECIDE msg
							PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
							message.setLogPositionNumber((msg.getLogPositionNumber() != 0)?msg.getLogPositionNumber():logCounter++);
							sendDecideMsg(message);

							if(isLeader)
							{
								//send response for the Paxos Instance
								sendPaxosInstanceResponse(msg);
								System.out.println("Decided on a value, sent response to TPC ");
							}
							System.out.println("Releasing the resources");
							releaseLocks(paxInstance.getAcceptedValue(), msg.getUID().substring(1));
						}
						else{
							//send client response
							///FIX me . send to TPC
							TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
							System.out.println("Sending client response(ABORT) to "+tempTransSource.getSource());
							//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
							//SendClientMessage(message, tempTransSource.getSource());
							TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT);
							SendTwoPCMessage(message, msg.getSource());
						}


					}
					else{
						System.out.println("Reached majority and already DECIDED. No action taken");
					}
				}
			}
		}
		else{
			System.out.println("First time seeing ACCEPT msg. Accepting the value and sending ACCEPT to all");


			PaxosInstance paxInstance = new PaxosInstance(uid, msg.getBallotNumber(), msg.getAcceptValue());
			paxInstance.addtoAcceptList(msg.getSource());
			paxInstance.addtoAcceptList(nodeAddress);
			boolean isWriteLock = true;
			for(ElementProto element : paxInstance.getAcceptedValue().getElementsList())
			{
				if(!lockTable.acquireLock(element.getRow(), uid.substring(1)))
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

	private void releaseLocks(ElementsSetProto elementsSetProto, String uid)
	{
		for(ElementProto element : elementsSetProto.getElementsList())
		{
			lockTable.releaseLock(element.getRow(), uid);
		}
		System.out.println("Released all resources. No ack sent");

	}

	private void sendPaxosInstanceResponse(PaxosMsg msg)
	{
		//send response to TPC
		TransactionSource tempTransSource = uidTransMap.get(msg.getUID());

		if(tempTransSource.getType() == TwoPCMsgType.PREPARE)
		{
			System.out.println("Sending PrepareAck to TwoPC "+tempTransSource.getSource());
			//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.WRITE_RESPONSE);
			//SendClientMessage(message, tempTransSource.getSource());

			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.PREPARE);
			SendTwoPCMessage(message, tempTransSource.getSource());
			System.out.println("Sending Prepare Ack for UID - "+msg.getUID());		
			LOGGER.log(Level.FINE, new String("Sent Prepare Ack for UID - "+msg.getUID()));
		}
		else if(tempTransSource.getType() == TwoPCMsgType.COMMIT){
			System.out.println("Sending Commit Ack to "+tempTransSource.getSource());
			//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
			//SendClientMessage(message, tempTransSource.getSource());
			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.COMMIT);
			SendTwoPCMessage(message, tempTransSource.getSource());
			System.out.println("Sending Commit Ack for UID - "+msg.getUID());		
			LOGGER.log(Level.FINE, new String("Sent Commit Ack for UID - "+msg.getUID()));
		}

		else if(tempTransSource.getType() == TwoPCMsgType.ABORT){
			System.out.println("Sending Abort Ack to "+tempTransSource.getSource());
			//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
			//SendClientMessage(message, tempTransSource.getSource());

			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT);
			SendTwoPCMessage(message, tempTransSource.getSource());
			System.out.println("Sending Abort Ack for UID - "+msg.getUID());		
			LOGGER.log(Level.FINE, new String("Sent Abort Ack for UID - "+msg.getUID()));
		}

		/*
		System.out.println("Sending client response(COMMIT) to "+tempTransSource.getSource());
		ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
		SendClientMessage(message, tempTransSource.getSource());*/
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

	public static void main(String args[]) throws IOException
	{
		if(args.length <= 1)
			throw new IllegalArgumentException("Usage: PAcceptor <ShardID> <nodeId>");
		PaxosAcceptor acceptor = new PaxosAcceptor(args[0], args[1]);
		new Thread(acceptor).start();
		//acceptor.readStreamMessages();
		acceptor.checkForPendingTrans();
	}

}
