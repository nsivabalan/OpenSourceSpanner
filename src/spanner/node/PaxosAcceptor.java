package spanner.node;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.ResourceHM;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.PLeaderState;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.ReplayMsgType;
import spanner.common.Common.TwoPCMsgType;
import spanner.locks.LockTable;
import spanner.locks.LockTableOld;
import spanner.message.ClientOpMsg;
import spanner.message.PaxosDetailsMsg;
import spanner.message.PaxosMsg;
import spanner.message.ReplayMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.ElementsSetProtoOrBuilder;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionProto;
import spanner.protos.Protos.TransactionProto.TransactionStatusProto;

public class PaxosAcceptor extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket publisher = null;
	ZMQ.Socket socket = null;
	NodeProto nodeAddress = null;
	NodeProto leaderAddress = null;
	int myId;
	NodeProto metadataService ;
	boolean isLeader ;
	String shard ;
	PLeaderState state ;
	ArrayList<NodeProto> acceptors;
	LockTable lockTable = null;
	private static File PAXOSLOG = null;
	private static FileWriter paxosLogWriter = null;
	private static BufferedWriter bufferedWriter = null;
	int acceptorsCount = 0;
	BallotNumber ballotNo = null;
	private HashMap<String, Integer> uidTologPositionMap = null; 
	private HashMap<Integer, String> logPostionToUIDMap = null;
	private ConcurrentHashMap<String, PaxosInstance> uidPaxosInstanceMap = null;
	private ConcurrentHashMap<Integer, PaxosInstance> logPositionToPaxInstanceMap = null;
	private HashSet<Integer> pendingPaxosInstances = null;
	private HashMap<String, TransactionSource> uidTransMap = null;
	private PaxosInstance dummyInstance = null;
	TwoPC twoPhaseCoordinator = null;
	private int logCounter = 0;
	RandomAccessFile logRAF = null;
	private static ResourceHM localResource = null;
	private static FileHandler logFile = null;

	public PaxosAcceptor(String shard, String nodeId, String clear) throws IOException
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
		createLogFile(shard, nodeId, clear);

		//socket.connect(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));

		//socket.bind(Common.getLocalAddress(Integer.parseInt(hostdetails[1])));
		socket.bind("tcp://127.0.0.1:"+hostDetails[1]);
		InetAddress addr = InetAddress.getLocalHost();
		nodeAddress = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(Integer.parseInt(hostDetails[1])).build();
		twoPhaseCoordinator = new TwoPC(nodeAddress, context);
		new Thread(twoPhaseCoordinator).start();

		System.out.println("Participant node address ****** "+nodeAddress);
		String[] mds = Common.getProperty("mds").split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		//FIX ME: check what needs to be passed in as constructor
		lockTable = new LockTable("");
		pendingPaxosInstances = new HashSet<Integer>();
		uidTransMap = new HashMap<String, TransactionSource>();
		localResource = new ResourceHM(this.LOGGER);
		//to fix: remove after testing
		if(nodeId.equalsIgnoreCase("1") || nodeId.equalsIgnoreCase("4"))
			isLeader = true;
		dummyInstance = new PaxosInstance(null,  null);
		state = PLeaderState.INIT;
		//state = PLeaderState.ACTIVE;
		myId = Integer.parseInt(nodeId);
		ballotNo = new BallotNumber(0, myId);
		uidPaxosInstanceMap = new ConcurrentHashMap<String, PaxosInstance>();
		logPositionToPaxInstanceMap = new ConcurrentHashMap<Integer, PaxosInstance>();
		uidTologPositionMap = new HashMap<String, Integer>();
		logPostionToUIDMap = new HashMap<Integer, String>();
		sendPaxosMsgRequestingAcceptors();
		sendPaxosMsgRequestingLeader(clear);
	}

	private class LogPositionDetail{
		int logPosition;
		boolean isWritten;

		public LogPositionDetail(int logPosition)
		{
			this.logPosition = logPosition;
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
	private void createLogFile(String shard, String nodeId, String isClear) throws NumberFormatException, IOException
	{
		PAXOSLOG = new File(Common.PaxosLog+"/"+shard+"/"+nodeId+"_.log");
		System.out.println("Creating log file "+PAXOSLOG.getAbsolutePath());
		if(PAXOSLOG.exists())
		{
			System.out.println("Already exists");
			if(isClear.equalsIgnoreCase("T"))
				new FileOutputStream(PAXOSLOG, false).close();			
		}
		else{
			System.out.println("Creating new file");
			PAXOSLOG.createNewFile();
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
			buffer.append("::");
		}

		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(PAXOSLOG, true)));
			out.println(buffer.toString());
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void handleReplayLogResponseMsg(ReplayMsg msg)
	{
		System.out.println("Handling response for replay msg");
		ArrayList<ReplayLogEntry> logEntries = msg.getAllReplayEntries();
		int lastLogPos = getLastPaxosInstanceNumber();
		boolean validFlag = true;
		int size = logEntries.size();
		if(size == 0)
			this.state = PLeaderState.ACTIVE;
		else
		{
			for(int i=0;i<size;i++)
			{
				ReplayLogEntry logEntry = logEntries.get(i);
				if(i == 0)
				{
					int curLogPos = logEntry.getLogPosition();
					if(curLogPos <= lastLogPos){
						requestReplayLog(lastLogPos, leaderAddress);
						validFlag = false;
						break;
					}
					else{
						replayLog(logEntry);
					}
				}
				else{
					replayLog(logEntry);
				}
			}
			if(validFlag){
				ReplayLogEntry lastEntry = logEntries.get(size-1);
				logCounter = lastEntry.getLogPosition() +1;
				this.state = PLeaderState.ACTIVE;
			}
		}
	}

	private void replayLog(ReplayLogEntry logEntry)
	{	
		writeToPaxLogFile(logEntry.getLogPosition(), logEntry.getOperation(), logEntry.getLogEntry());
		if(logEntry.getOperation().equalsIgnoreCase("COMMITED"))
			localResource.WriteResource(logEntry.getLogEntry());
	}

	private void handleReplayLogRequestMsg(ReplayMsg msg){
		System.out.println("Handling request for replay msg ");
		SendReplayLog(msg.getLastLogPosition(), msg.getSource());
	}

	private void SendReplayLog(int logPosition, NodeProto dest) 
	{
		try {

			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(PAXOSLOG)));

			ReplayMsg msg = new ReplayMsg(nodeAddress, ReplayMsgType.RESPONSE);
			String line = br.readLine();
			while(line != null)
			{
				String[] contents = line.split("=");
				String[] posOprtn = contents[0].split(":");
				int logPos = Integer.parseInt(posOprtn[0]);
				if(logPos > logPosition){

					ElementsSetProto.Builder logEntry = ElementsSetProto.newBuilder();
					String[] records = contents[1].split("::");
					for(String record: records)
					{
						ElementProto.Builder rowBuilder = ElementProto.newBuilder();
						String[] recordSplit = record.split(":");
						rowBuilder.setRow(recordSplit[0]);
						String[] cols = recordSplit[1].split(";");
						for(String col: cols)
						{
							String[] colSplit = col.split(",");
							rowBuilder.addCols(ColElementProto.newBuilder().setCol(colSplit[0]).setValue(colSplit[1]).build());
						}
						logEntry.addElements(rowBuilder.build());
					}
					ReplayLogEntry replayLogEntry = new ReplayLogEntry(logPos, posOprtn[1], logEntry.build());
					msg.addLogEntry(replayLogEntry);
				}
				line = br.readLine();
			}
			br.close();
			sendReplayLogMsg(dest, msg);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private int getLastPaxosInstanceNumber()
	{
		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(PAXOSLOG)));

			String line = null;
			String prevLine = null ;

			line = br.readLine();

			while(line != null)
			{
				prevLine = line;
				line = br.readLine();
			}
			br.close();
			if(prevLine == null || prevLine.equalsIgnoreCase(""))
				return -1;
			else{
				String[] contents = prevLine.split("=");
				String[] logPosOprtn = contents[0].split(":");

				return Integer.parseInt(logPosOprtn[0]);
			}
		}catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
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

	private void sendPaxosMsgRequestingLeader( String isNew)
	{
		if(isNew.equalsIgnoreCase("T"))
		{
			state = PLeaderState.ACTIVE;
		}
		else{
			PaxosDetailsMsg msg = new PaxosDetailsMsg(nodeAddress, shard, PaxosDetailsMsgType.LEADER);
			sendMsgToMDS(metadataService ,msg);
		}
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

	private void sendReplayLogMsg(NodeProto dest, ReplayMsg message)
	{
		System.out.println("Sent " + message+" to "+dest.getHost()+":"+dest.getPort());
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


	public void executeDaemon()
	{
		while(true){
			checkForPendingTrans();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private synchronized void checkForPendingTrans() 
	{

		Long curTime = new Date().getTime();
		HashSet<Integer> pendingInstancesTemp = (HashSet<Integer>)pendingPaxosInstances.clone();
		for(Integer logPos: pendingInstancesTemp)
		{
			System.out.println("LogPos %%%%%%%%%%%% "+logPos);
			//PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
			PaxosInstance paxInstance = logPositionToPaxInstanceMap.get(logPos);
			String uid = paxInstance.getUID();
			System.out.println("PaxInstance %%%%%%%%%%%% "+paxInstance);
			if(curTime - paxInstance.getTimeStamp() > Common.TRANS_TIMEOUT)
			{	
				System.out.println("Transaction timed out.");

				if(paxInstance.decides.size() == 0)
				{
					System.out.println("Total ACCEPTs haven't reached majority. Hence aborting the trans "+uid);
					if(uid.startsWith("P")){
						System.out.println("PREPARE txn "+uid+" hasn't reached majority. Releasing all locks obtained");
						pendingPaxosInstances.remove(logPos);
						releaseLocks(paxInstance.getAcceptedValue(), uid);
					}
					else if(uid.startsWith("C")){
						System.out.println("COMMIT txn "+uid +" hasn't reached majority. Hence aborting the transaction");
						uidPaxosInstanceMap.put(uid, paxInstance);
						pendingPaxosInstances.remove(logPos);
						releaseLocks(paxInstance.getAcceptedValue(), uid);
						//FIX ME: check if paxos leader should respond to the TPC or rely on TPC timeouts
						TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(uid).getTrans(), TwoPCMsgType.ABORT, true);
						SendTwoPCMessage(message, uidTransMap.get(uid).getSource());
					}
					else if(uid.startsWith("A"))
					{
						System.out.println("ABORT txn "+uid+" hasn't reached majority. No action taken");
						pendingPaxosInstances.remove(logPos);
					}
				}
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
		System.out.println("Sent " + msg+" from "+nodeAddress.getHost()+":"+nodeAddress.getPort() +" to "+dest.getHost()+":"+dest.getPort()+"\n");
		this.AddLogEntry("Sent "+msg+"\n", Level.INFO);

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
			//	if(isLeader)	System.out.println("Msg wrap updated ========================== "+receivedMsg);
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
						if(isLeader) System.out.println("Paxos msg received ++++++++++ "+msg);
						if(msg.getType() == PaxosMsgType.PREPARE)
						{
							//handlePaxosPrepareMessage(msg);
						}
						else if(msg.getType() == PaxosMsgType.ACK)
						{
							//	handlePaxosAckMessage(msg);
							System.out.println("ACK msg received. Not expected %%%%%%%%%%%%%%%%%%%%%% "+msg);
						}
						else if(msg.getType() == PaxosMsgType.ACCEPT)
						{

							handlePaxosAcceptMessage(msg);
						}
						else if(msg.getType() == PaxosMsgType.DECIDE)
						{
							System.out.println("DECIDE MSG from "+msg.getSource().getHost()+":"+msg.getSource().getPort());
							handlePaxosDecideMessage(msg);
						}
						else{
							System.out.println("Paxos Msg :: Not falling in any case. ERROR %%%%%%%%%%%%%%%%%%%%%%% "+msg+" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5");
						}
					}
					if(msgwrap.getmessageclass() == PaxosDetailsMsg.class )
					{
						PaxosDetailsMsg msg = (PaxosDetailsMsg)msgwrap.getDeSerializedInnerMessage();
						if(msg.getMsgType() == PaxosDetailsMsgType.ACCEPTORS)
							handlePaxosDetailsAcceptorsMsg(msg);
						else
							handlePaxosDetailsLeaderMsg(msg);
					}
					if(msgwrap.getmessageclass() == ReplayMsg.class )
					{
						ReplayMsg msg = (ReplayMsg)msgwrap.getDeSerializedInnerMessage();
						System.out.println("Received replay msg ^^^^^^^^^^^^^^^^^^^^^^^ ");
						if(msg.getMsgType() == ReplayMsgType.REQEUST)
							handleReplayLogRequestMsg(msg);
						else
							handleReplayLogResponseMsg(msg);
					}

					if(msgwrap.getmessageclass() == TwoPCMsg.class )
					{
						System.out.println("Two PC msg received ..... ");
						TwoPCMsg msg = (TwoPCMsg) msgwrap.getDeSerializedInnerMessage();

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
						else{
							System.out.println(" TwoPC msg: Not falling in any case. ERROR %%%%%%%%%%%%%%%%%%%%%%% "+msg);
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
			if(!lockTable.getReadLock(element.getRow(), trans.getTransactionID()))
				isReadLock = false;
		}

		System.out.println("Acquired all read locks."+isReadLock);
		ClientOpMsg read_response = null;
		if(isReadLock){
			ElementsSetProto readValues = localResource.ReadResource(trans.getReadSet());

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


	private void handleClientReleaseResourceMsg(ClientOpMsg message)
	{
		System.out.println("Inside process client release resource msg");
		TransactionProto trans = message.getTransaction();


		//FIX ME: check if commited or aborted trans
		lockTable.releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), true); 

		System.out.println("Released all resources. No ack sent");

	}

	private void handleClientWriteMessage(ClientOpMsg msg)
	{
		//System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();

			System.out.println("New write trans from ---------- "+msg.getSource().getHost()+":"+msg.getSource().getPort());
			//uidTransMap.put("P"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));
			boolean isWriteLock = true;
			for(ElementProto element : trans.getWriteSet().getElementsList())
			{
				if(!lockTable.getWriteLock(element.getRow(), trans.getTransactionID()))
				{
					isWriteLock = false;
				}
			}
			System.out.println("IsWritelock Acquired "+isWriteLock);
			if(isWriteLock){
				//Already leader. Send Accept right Away
				int newLogPosition = logCounter++;
				System.out.println("!!!!!!!!!! client write msg ");
				uidTologPositionMap.put("P"+trans.getTransactionID(), newLogPosition);
				logPostionToUIDMap.put(newLogPosition, "P"+trans.getTransactionID());
				PaxosInstance paxInstance = new PaxosInstance("P"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				//paxInstanceToUIDMap.put("P"+trans.getTransactionID(), trans.getTransactionID());
				//uidPaxosInstanceMap.put("P"+trans.getTransactionID(), paxInstance);
				//logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				uidTransMap.put( "P"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.PREPARE));
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				
				paxInstance.addtoAcceptList(nodeAddress);
				pendingPaxosInstances.add(newLogPosition);
				uidPaxosInstanceMap.put("P"+trans.getTransactionID(), paxInstance);
				logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				System.out.println("Sending accept msgs to all acceptors ");
				PaxosMsg message = new PaxosMsg(nodeAddress, "P"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				sendAcceptMsg(message);
			}
			else{
				System.out.println("Not able to acquire locks. Aborting the trans");
				lockTable.releaseLocksOfAborted(trans);
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

	
	private void initiatePreparePhase()
	{
		int newLogPosition = logCounter;
		PaxosInstance paxInstance = new PaxosInstance("-1", ballotNo);
		
	}

	
	
	private void SendPrepareMessage(PaxosMsg message)
	{
		SendMessageToAcceptors(message);
	}
	
	private void SendMessageToAcceptors(PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!nodeAddress.equals(node))
				sendPaxosMsg(node, msg);
		}
	}
	
	private void handleTwoPCCommitMessage(TwoPCMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();

			System.out.println("New commit trans from TPC :: ---------- "+msg.getSource().getHost()+":"+msg.getSource().getPort());
			//uidTransMap.put("P"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));
			//FIX ME: is the below block required
			boolean isWriteLock = true;
			for(ElementProto element : trans.getWriteSet().getElementsList())
			{
				System.out.println("Trying to acquire lock for trans "+trans.getTransactionID());
				if(!lockTable.getWriteLock(element.getRow(), trans.getTransactionID()))
				{
					isWriteLock = false;
				}
			}
			System.out.println("IsWritelock Acquired "+isWriteLock);
			if(isWriteLock){
				//Already leader. Send Accept right Away
				int newLogPosition = logCounter++;
				System.out.println("!!!!!!!!!! Two PC commit msg");
				uidTologPositionMap.put("C"+trans.getTransactionID(), newLogPosition);
				logPostionToUIDMap.put(newLogPosition, "C"+trans.getTransactionID());
				PaxosInstance paxInstance = new PaxosInstance("C"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				uidTransMap.put( "C"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.COMMIT));

				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				pendingPaxosInstances.add(newLogPosition);
				paxInstance.addtoAcceptList(nodeAddress);
				uidPaxosInstanceMap.put("C"+trans.getTransactionID(), paxInstance);
				logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				PaxosMsg message = new PaxosMsg(nodeAddress, "C"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			}
			else{
				System.out.println("Not able to acquire locks. Aborting the trans");

				//ClientOpMsg message = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.ABORT);
				//SendClientMessage(message, msg.getSource());
				//Fix me
				lockTable.releaseLocksOfAborted(trans);
				TwoPCMsg message = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
		else{
			//yet to fill in
		}
	}


	private synchronized void handleTwoPCAbortMessage(TwoPCMsg msg)
	{
		System.out.println("IsLeader --------- "+isLeader);
		if(isLeader)
		{	
			TransactionProto trans = msg.getTransaction();
			String uid = trans.getTransactionID();
			if(!uidPaxosInstanceMap.contains("A"+uid)){
				
				
				if(pendingPaxosInstances.contains(uidTologPositionMap.get("P"+uid)))
				{
					System.out.println("Aborting paxInstance for PREPARE for the same trans ");
					pendingPaxosInstances.remove(uidTologPositionMap.get("P"+uid));
				}

				System.out.println("New Abort trans from ---------- "+msg.getSource().getHost()+":"+msg.getSource().getPort());
				System.out.println("!!!!!!!!!! TwoPC ABORT msg");
				int newLogPosition = logCounter++;
				uidTologPositionMap.put("A"+trans.getTransactionID(), newLogPosition);
				logPostionToUIDMap.put(newLogPosition, "A"+trans.getTransactionID());
				writeToPaxLogFile(newLogPosition, "ABORTED", trans.getWriteSet());
				PaxosInstance paxInstance = new PaxosInstance("A"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				//paxInstanceToUIDMap.put("P"+trans.getTransactionID(), trans.getTransactionID());
				uidTransMap.put( "A"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.ABORT));
				lockTable.releaseLocksOfAborted(trans);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				pendingPaxosInstances.add(newLogPosition);
				paxInstance.addtoAcceptList(nodeAddress);
				
				System.out.println("Adding paxIsntace for Abort ******** "+paxInstance);
				uidPaxosInstanceMap.put("A"+trans.getTransactionID(), paxInstance);
				logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				PaxosMsg message = new PaxosMsg(nodeAddress, "A"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				System.out.println("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);

			}
			else{
				System.out.println("Already Trans aborted. No action taken");
			}
		}
		else{
			//yet to fill in
		}
	}

	private synchronized void handlePaxosDecideMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		System.out.println("$$$$$$$$$$$$$$$ Handling DECIDE msg for "+uid+"  from :: "+msg.getSource().getHost()+":"+msg.getSource().getPort()+" $$$$$$$$$$$$ ");
		
		/*System.out.println("List of entires in logPositiontoPI map");
		for(Integer pos: logPositionToPaxInstanceMap.keySet())
		{
			System.out.println(" "+pos+" "+logPositionToPaxInstanceMap.get(pos));
		}
		System.out.println("Done ");*/ 
		
		System.out.println(" logPosition "+msg.getLogPositionNumber());
		if(logPositionToPaxInstanceMap.containsKey(msg.getLogPositionNumber()))
		{
			//System.out.println("Handling decide msg inside 1st if loop");
			//System.out.println("Already ACCEPT sent to all. Adding to the list of acceptors");
			PaxosInstance paxInstance = logPositionToPaxInstanceMap.get(msg.getLogPositionNumber());
			System.out.println(" paxIsntance "+paxInstance);
			int prevCount = paxInstance.decides.size();
			paxInstance.decides.add(msg.getSource());
			System.out.print("List of decides <<<<< ");
			for(NodeProto nodeProto: paxInstance.decides)
			{
				System.out.print(nodeProto.getHost()+":"+nodeProto.getPort()+";");
			}
			System.out.println(" >>>>>>>>>>>>>>>>>>>>");

			int newCount = paxInstance.decides.size();
			if(prevCount < newCount ){
				if(!paxInstance.isDecideSent)
				{

					paxInstance.setTimeStamp(new Date().getTime());
					paxInstance.isDecideSent = true;
					paxInstance.addToDecideList(nodeAddress);
					System.out.println("AcceptValue decided to be :: \n");

					for(ElementProto elementProto:  paxInstance.getAcceptedValue().getElementsList())
					{
						String temp = elementProto.getRow()+":";
						for(ColElementProto colElemProto: elementProto.getColsList())
						{
							temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
						}
						System.out.println(temp+"\n");
					}
					paxInstance.setCommited();
					uidPaxosInstanceMap.put(uid, paxInstance);
					logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
					pendingPaxosInstances.remove(msg.getLogPositionNumber());
					initiateDecide(msg);
				}
				else{
					System.out.println("Reached majority and already DECIDED. No action taken");
				}
			}
			else{
				System.out.println("Already seen DECIDE from the same acceptor &&&&& ");
			}
		}
		else{
			System.out.println("First time seeing DECIDE msg. DECIDING the value and sending DECIDE to all");
			PaxosInstance paxInstance  = null;
			if(logPositionToPaxInstanceMap.containsKey(msg.getLogPositionNumber())){
				paxInstance = logPositionToPaxInstanceMap.get(msg.getLogPositionNumber());
			}
			else{
				paxInstance = new PaxosInstance(uid, msg.getBallotNumber(), msg.getAcceptNo(), msg.getAcceptValue());

			}

			paxInstance.addToDecideList(msg.getSource());
			paxInstance.addToDecideList(nodeAddress);
			paxInstance.setTimeStamp(System.currentTimeMillis());
			paxInstance.isDecideSent = true;
			paxInstance.isCommited = true;
			uidPaxosInstanceMap.put(uid, paxInstance );
			logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
			if(pendingPaxosInstances.contains(msg.getLogPositionNumber()))
				pendingPaxosInstances.remove(msg.getLogPositionNumber());
			initiateDecide(msg);
		}
	}

	private synchronized void handlePaxosAcceptMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		System.out.println("Handling ACCEPT msg for "+uid+" from :: "+msg.getSource().getHost()+":"+msg.getSource().getPort());
		
			if(logPostionToUIDMap.containsKey(msg.getLogPositionNumber()))
			{	
				System.out.println("Already having paxInstance for the same log Position");
				PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
				if((msg.getBallotNumber().compareTo(paxInstance.getBallotNumber())) >=0 ){
					paxInstance.setBallotNumber(msg.getBallotNumber());
					int prevCount = paxInstance.accepts.size();
					paxInstance.accepts.add(msg.getSource());
					int newCount = paxInstance.accepts.size();
					if(newCount > prevCount){
						if(newCount >  acceptorsCount/2)
						{
							if(!paxInstance.isDecideSent)
							{
								System.out.println("Reached majority of acceptors, but DECIDE not yet sent. DECIDING and sending DECIDE msg");						
								paxInstance.isDecideSent = true;
								paxInstance.addToDecideList(nodeAddress);
								if(pendingPaxosInstances.contains(msg.getLogPositionNumber()))
									pendingPaxosInstances.remove(msg.getLogPositionNumber());

								System.out.println("AcceptValue decided to be :: \n");

								for(ElementProto elementProto:  paxInstance.getAcceptedValue().getElementsList())
								{
									String temp = elementProto.getRow()+":";
									for(ColElementProto colElemProto: elementProto.getColsList())
									{
										temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
									}
									System.out.println(temp+"\n");
								}
								paxInstance.setCommited();
								uidPaxosInstanceMap.put(uid, paxInstance);
								System.out.println("====================== Adding pax instance to log position "+msg.getLogPositionNumber()+" "+paxInstance);
								logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
								initiateDecide(msg);

							}
							else{
								System.out.println("Reached majority and already DECIDED. No action taken");
							}
						}
					}
				}
				else{
					System.out.println("Already accepted BN is greater than the received one. My cur BN: "+paxInstance.getBallotNumber()+", received "+msg.getBallotNumber());
				}
			}
			else{
				System.out.println("First time seeing ACCEPT msg. Accepting the value and sending ACCEPT to all");

				
				PaxosInstance paxInstance = new PaxosInstance(uid, msg.getBallotNumber(), msg.getAcceptValue());
				paxInstance.addtoAcceptList(msg.getSource());
				paxInstance.addtoAcceptList(nodeAddress);
				paxInstance.setTimeStamp(new Date().getTime());
				
				boolean isWriteLock = true;
				for(ElementProto element : paxInstance.getAcceptedValue().getElementsList())
				{
					if(!lockTable.getWriteLock(element.getRow(), uid.substring(1)))
					{
						isWriteLock = false;
					}

				}

				if(isWriteLock){
					paxInstance.setAcceptSent();
					uidPaxosInstanceMap.put(uid, paxInstance );
					
					int newLogPosition = logCounter++;
					System.out.println("!!!!!!!!!! First time ACCEPT "+newLogPosition+" "+paxInstance);
					uidTologPositionMap.put(uid, newLogPosition);
					logPostionToUIDMap.put(newLogPosition, uid);
					logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
					if(!pendingPaxosInstances.contains(newLogPosition)){
						pendingPaxosInstances.add(newLogPosition);
					}
					
					PaxosMsg message = new PaxosMsg(nodeAddress, uid,PaxosMsgType.ACCEPT, msg.getBallotNumber(),msg.getAcceptValue());
					message.setLogPositionNumber(msg.getLogPositionNumber());
					sendAcceptMsg( message );
				}
				else{
					uidPaxosInstanceMap.put(uid, paxInstance );
					uidTransMap.remove(uid);
					
					dummyInstance = new PaxosInstance("A"+uid.substring(1), null);
					dummyInstance.setAcceptedValue(msg.getAcceptValue());
					uidPaxosInstanceMap.put("A"+uid.substring(1), dummyInstance);
					System.out.println("!!!!!!!!!! First time accept, cannot acquire locks");
					int newLogPosition = logCounter++;
					writeToPaxLogFile(newLogPosition, "ABORTED", paxInstance.getAcceptedValue());
					logPositionToPaxInstanceMap.put(newLogPosition, dummyInstance);
					TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
					System.out.println("Sending client response(ABORT) to "+tempTransSource.getSource());
					lockTable.releaseLocksOfAborted(tempTransSource.trans);
					//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
					//SendClientMessage(message, tempTransSource.getSource());
					TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
					SendTwoPCMessage(message, msg.getSource());

				}
			}
		

	}

	private void initiateDecide(PaxosMsg msg)
	{

		String uid = msg.getUID();
		PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
		if(uid.startsWith("C")){
			Boolean isWritten = localResource.WriteResource(paxInstance.getAcceptedValue());

			if(isWritten){
				writeToPaxLogFile(uidTologPositionMap.get(uid), "COMMITED", paxInstance.getAcceptedValue());
				System.out.println("Releasing the resources");
				releaseLocks(paxInstance.getAcceptedValue(), uid.substring(1));
				System.out.println("Trans id "+uid.substring(1));
				System.out.println("Key set "+uidTransMap.keySet());
				if(uidTransMap.containsKey(uid.substring(1))){
					ElementsSetProto readSet = uidTransMap.get(uid.substring(1)).trans.getReadSet();
					System.out.println("Read set "+readSet+" for trans "+uid.substring(1));
					if(readSet != null)
						lockTable.releaseReadLocks(uidTransMap.get(uid.substring(1)).trans.getReadSet(), uid.substring(1), true);
					//FIX ME: send DECIDE msg to participants
				}
				System.out.println("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY - C");
				PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
				message.setLogPositionNumber(msg.getLogPositionNumber());
				sendDecideMsg(message);
				if(isLeader)
				{
					//send response for the Paxos Instance
					System.out.println("Decided on a value(Commited), sent response to TPC ");

					sendPaxosInstanceResponse(msg);

				}

			}
			else{
				//send client response
				///FIX me . send to TPC
				writeToPaxLogFile(uidTologPositionMap.get(uid), "ABORTED", paxInstance.getAcceptedValue());
				if(isLeader){
					TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
					System.out.println("Sending client response(ABORT) to "+tempTransSource.getSource());
					//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
					//SendClientMessage(message, tempTransSource.getSource());
					lockTable.releaseLocksOfAborted(tempTransSource.trans);
					TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
					SendTwoPCMessage(message, msg.getSource());
				}
				//writeToPaxLogFile(++logCounter, "ABORT", paxInstance.getAcceptedValue());
			}
		}
		else if(uid.startsWith("P")){//Pax instance for PREPARE phase. Just obtained read locks
			System.out.println("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY - P");
			//FIX ME: send DECIDE msg to participants
			writeToPaxLogFile(uidTologPositionMap.get(uid), "PREPARED", paxInstance.getAcceptedValue());
			PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
			message.setLogPositionNumber(msg.getLogPositionNumber());
			sendDecideMsg(message);
			if(isLeader){
				System.out.println("Done with PREPARE phase, sent response to TPC ");
				sendPaxosInstanceResponse(msg);
			}
		}
		else if(uid.startsWith("A"))
		{
			//FIX ME: send DECIDE msg to participants
			System.out.println("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY - A");

			//	writeToPaxLogFile(uidTologLocationMap.get(uid).logPosition, "ABORTED", paxInstance.getAcceptedValue());
			lockTable.releaseLocksOfAborted(uidTransMap.get(uid).trans);
			PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
			message.setLogPositionNumber(msg.getLogPositionNumber());
			sendDecideMsg(message);
			if(isLeader){
				System.out.println("Done with ABORT, sent response to TPC ");
				sendPaxosInstanceResponse(msg);
			}
		}
	}

	private void releaseLocks(ElementsSetProto elementsSetProto, String uid)
	{
		for(ElementProto element : elementsSetProto.getElementsList())
		{
			lockTable.releaseWriteLock(element.getRow(), uid, true);
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

			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.PREPARE, true);
			SendTwoPCMessage(message, tempTransSource.getSource());
			System.out.println("Sending Prepare Ack for UID - "+msg.getUID());		
			LOGGER.log(Level.FINE, new String("Sent Prepare Ack for UID - "+msg.getUID()));
		}
		else if(tempTransSource.getType() == TwoPCMsgType.COMMIT){
			System.out.println("Sending Commit Ack to "+tempTransSource.getSource());
			//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
			//SendClientMessage(message, tempTransSource.getSource());
			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.COMMIT, true);
			SendTwoPCMessage(message, tempTransSource.getSource());
			System.out.println("Sending Commit Ack for UID - "+msg.getUID());		
			LOGGER.log(Level.FINE, new String("Sent Commit Ack for UID - "+msg.getUID()));
		}

		else if(tempTransSource.getType() == TwoPCMsgType.ABORT){
			System.out.println("Sending Abort Ack to "+tempTransSource.getSource());
			//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
			//SendClientMessage(message, tempTransSource.getSource());

			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
			SendTwoPCMessage(message, tempTransSource.getSource());
			System.out.println("Sending Abort Ack for UID - "+msg.getUID());		
			LOGGER.log(Level.FINE, new String("Sent Abort Ack for UID - "+msg.getUID()));
		}

		/*
		System.out.println("Sending client response(COMMIT) to "+tempTransSource.getSource());
		ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.COMMIT);
		SendClientMessage(message, tempTransSource.getSource());*/
	}

	private void handlePaxosDetailsAcceptorsMsg(PaxosDetailsMsg msg)
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
		if(isLeader)
			this.state = PLeaderState.ACTIVE;
		//	initPaxosInstance();
	}

	private void handlePaxosDetailsLeaderMsg(PaxosDetailsMsg msg)
	{
		leaderAddress = msg.getShardLeader();
		int logPosition = getLastPaxosInstanceNumber();
		if(!isLeader){
			System.out.println("Last log position "+logPosition);
			requestReplayLog(logPosition, leaderAddress);
		}
	}

	private void requestReplayLog(int logPosition, NodeProto dest)
	{
		ReplayMsg msg = new ReplayMsg(nodeAddress, ReplayMsgType.REQEUST);
		msg.setLastLogPosition(logPosition);
		sendReplayLogMsg(dest, msg);
	}

	private void SendTwoPCMessage(TwoPCMsg message, NodeProto dest)
	{
		//Print msg
		System.out.println("Sending TwoPCMsg " + message+" +to "+dest.getHost()+":"+dest.getPort());
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
		if(args.length <= 2)
			throw new IllegalArgumentException("Usage: PAcceptor <ShardID> <nodeId> T/F(clear log file or no)");
		PaxosAcceptor acceptor = new PaxosAcceptor(args[0], args[1], args[2]);
		new Thread(acceptor).start();
		//acceptor.readStreamMessages();
		acceptor.executeDaemon();
	}

}
