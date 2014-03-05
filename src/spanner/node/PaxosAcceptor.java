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
import spanner.common.Resource;
import spanner.common.ResourceHM;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.LeaderMsgType;
import spanner.common.Common.PLeaderState;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.ReplayMsgType;
import spanner.common.Common.TwoPCMsgType;
import spanner.locks.LockTable;
import spanner.message.ClientOpMsg;
import spanner.message.LeaderMsg;
import spanner.message.MessageBase;
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
	ZMQ.Socket metaDataSocket = null;
	boolean isLeader ;
	String shard ;
	PLeaderState state ;
	ArrayList<NodeProto> acceptors;
	LockTable lockTable = null;
	private static boolean clearLog ;
	private static File PAXOSLOG = null;
	int acceptorsCount = 0;
	BallotNumber ballotNo = null;
	private HashMap<String, Integer> uidTologPositionMap = null; 
	private HashMap<Integer, String> logPostionToUIDMap = null;
	private ConcurrentHashMap<String, PaxosInstance> uidPaxosInstanceMap = null;
	private ConcurrentHashMap<Integer, PaxosInstance> logPositionToPaxInstanceMap = null;
	private HashSet<Integer> pendingPaxosInstances = null;
	private HashMap<String, TransactionSource> uidTransMap = null;
	private ClientOpMsg waitingRequest = null;
	private PaxosInstance dummyInstance = null;
	private ArrayList<MessageWrapper> pendingRequests = null;
	private HashSet<NodeProto> pendingReplayReplicas = null;
	TwoPC twoPhaseCoordinator = null;
	private ZMQ.Socket leaderSocket = null;
	private int logCounter = 0;
	RandomAccessFile logRAF = null;
	//private static ResourceHM localResource = null;
	private static Resource localResource = null;
	public HashMap<NodeProto, ZMQ.Socket> addressToSocketMap = null;
	private static FileHandler logFile = null;

	public PaxosAcceptor(String shard, String nodeId, boolean isNew) throws IOException
	{
		super(nodeId, isNew);
		//AddLogEntry("Logger ::::::::::::: PAXOS ACCEPTOR ::::::::::::; "+this.LOGGER);
		this.shard = shard;
		context = ZMQ.context(1);
		String[] hostDetails = Common.getProperty(nodeId).split(":");
		socket = context.socket(ZMQ.PULL); 
		//create Log file
		createLogFile(shard, nodeId, isNew);
		//FIX ME
		String hostName = null;

		socket.bind("tcp://*:"+hostDetails[1]);
		nodeAddress = NodeProto.newBuilder().setHost(hostDetails[0]).setPort(Integer.parseInt(hostDetails[1])).build();
		twoPhaseCoordinator = new TwoPC(shard, nodeAddress, context, isNew, LOGGER);
		new Thread(twoPhaseCoordinator).start();
		//AddLogEntry("local address " +InetAddress.getLocalHost().getHostAddress());
		AddLogEntry("Connected @ "+nodeAddress.getHost()+":"+nodeAddress.getPort(), Level.FINE);
		int mdsIndex = nodeId.charAt(0)-'0';

		String[] mds = Common.getProperty("mds_"+mdsIndex%3).split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();

		metaDataSocket = context.socket(ZMQ.PUSH);
		metaDataSocket.connect("tcp://"+metadataService.getHost()+":"+metadataService.getPort());
		//FIX ME: check what needs to be passed in as constructor
		lockTable = new LockTable(nodeId, isNew);
		pendingPaxosInstances = new HashSet<Integer>();
		uidTransMap = new HashMap<String, TransactionSource>();
		//localResource = new ResourceHM(this.LOGGER);
		localResource = new Resource(this.LOGGER);
		dummyInstance = new PaxosInstance(null,  null);
		state = PLeaderState.INIT;
		myId = Integer.parseInt(nodeId);
		ballotNo = new BallotNumber(0, myId);
		uidPaxosInstanceMap = new ConcurrentHashMap<String, PaxosInstance>();
		logPositionToPaxInstanceMap = new ConcurrentHashMap<Integer, PaxosInstance>();
		uidTologPositionMap = new HashMap<String, Integer>();
		logPostionToUIDMap = new HashMap<Integer, String>();
		pendingRequests = new ArrayList<MessageWrapper>();
		pendingReplayReplicas = new HashSet<NodeProto>();
		this.clearLog = isNew;
		addressToSocketMap = new HashMap<NodeProto, ZMQ.Socket>();
		sendPaxosMsgRequestingAcceptors();
		sendPaxosMsgRequestingLeader();
	}

	/**
	 * Class used to mainting mapping from transaction to transaction client and the current state of trans
	 * @author sivabalan
	 *
	 */
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

	/**
	 * Method to create Transactional log file
	 * @param shard
	 * @param nodeId
	 * @param isClear
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void createLogFile(String shard, String nodeId, boolean isClear) throws NumberFormatException, IOException
	{
		File paxosDir = new File(Common.PaxosLog);
		if(!paxosDir.exists())
			paxosDir.mkdirs();
		PAXOSLOG = new File(Common.PaxosLog+"/"+nodeId+"_.log");
		if(PAXOSLOG.exists())
		{
			if(isClear){				
				new FileOutputStream(PAXOSLOG, false).close();
				AddLogEntry("Clearing contents and creating new Log file "+PAXOSLOG.getAbsolutePath());
			}
			else{
				AddLogEntry("Appending logs to already existing log file "+PAXOSLOG.getAbsolutePath());
			}
		}
		else{
			PAXOSLOG.createNewFile();
			AddLogEntry("File does not exist. Hence creating new Log file "+PAXOSLOG.getAbsolutePath());
		}
	}


	/**
	 * Daemon thread executing in the background to check for aborts for pending transaction
	 */
	public void executeDaemon()
	{
		while(true){
			checkForPendingTrans();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Method used to check pending trnsactions for aborts
	 */
	private void checkForPendingTrans() 
	{

		Long curTime = new Date().getTime();
		HashSet<Integer> pendingInstancesTemp = (HashSet<Integer>)pendingPaxosInstances.clone();
		for(Integer logPos: pendingInstancesTemp)
		{
			PaxosInstance paxInstance = logPositionToPaxInstanceMap.get(logPos);
			String uid = paxInstance.getUID();
			if(curTime - paxInstance.getTimeStamp() > Common.TRANS_TIMEOUT)
			{	
				AddLogEntry("Transaction timed out "+paxInstance);

				if(pendingPaxosInstances.contains(logPos)){
					synchronized (this) {
						if(paxInstance.decides.size() == 0 && pendingPaxosInstances.contains(logPos))
						{

							AddLogEntry("Total ACCEPTs haven't reached majority. Hence aborting the trans "+uid);
							if(uid.startsWith("P")){
								AddLogEntry("PREPARE txn "+uid+" hasn't reached majority. Releasing all locks obtained");
								pendingPaxosInstances.remove(logPos);
								releaseLocks(paxInstance.getAcceptedValue(), uid);
							}
							else if(uid.startsWith("C")){
								AddLogEntry("COMMIT txn "+uid +" hasn't reached majority. Hence aborting the transaction");
								uidPaxosInstanceMap.put(uid, paxInstance);
								pendingPaxosInstances.remove(logPos);
								releaseLocks(paxInstance.getAcceptedValue(), uid);
								//FIX ME: check if paxos leader should respond to the TPC or rely on TPC timeouts
								TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(uid).getTrans(), TwoPCMsgType.ABORT, true);
								SendTwoPCMessage(message, uidTransMap.get(uid).getSource());
							}
							else if(uid.startsWith("A"))
							{
								AddLogEntry("ABORT txn "+uid+" hasn't reached majority. No action taken");
								pendingPaxosInstances.remove(logPos);
							}
						}
					}
				}
			}
			else{
				AddLogEntry("Checking txn "+logPos+" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			}
		}
	}


	public void run()
	{
		while (!Thread.currentThread().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();			
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);			
			handleIncomingMessage(msgwrap);
		}
		socket.close();
		for(NodeProto nodeProto : addressToSocketMap.keySet())
			addressToSocketMap.get(nodeProto).close();
		metaDataSocket.close();
		leaderSocket.close();
		context.term();
	}

	/**
	 * Method to handle incoming Messages
	 * @param msgwrap
	 */
	public synchronized void handleIncomingMessage(MessageWrapper msgwrap)
	{
		if (msgwrap != null ) 
		{

			try {
				if(msgwrap.getmessageclass() == ClientOpMsg.class)
				{
					ClientOpMsg msg = (ClientOpMsg) msgwrap.getDeSerializedInnerMessage();
					if(state == PLeaderState.ACTIVE){
						if(msg.getMsgType() == ClientOPMsgType.READ)
						{
							handleClientReadMessage(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.WRITE)
						{						
							handleClientWriteMessage(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.RELEASE_RESOURCE)
						{
							handleClientReleaseResourceMsg(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.UNLOCK)
						{
							handleClientReleaseReadSet(msg);
						}
						else if(msg.getMsgType() == ClientOPMsgType.ABORT)
						{
							twoPhaseCoordinator.handleClientAbortMsg(msg);
						}
					}
					else{
						pendingRequests.add(msgwrap);
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
					//	System.out.println("Received replay msg ^^^^^^^^^^^^^^^^^^^^^^^ ");
					if(msg.getMsgType() == ReplayMsgType.REQEUST)
						processReplayLogRequestMsg(msg);
					else if(msg.getMsgType() == ReplayMsgType.RESPONSE)
						processReplayLogResponseMsg(msg);
					else
						processReplayAck(msg);
				}

				if(msgwrap.getmessageclass() == PaxosMsg.class )
				{
					PaxosMsg msg = (PaxosMsg)msgwrap.getDeSerializedInnerMessage();
					//	if(isLeader) System.out.println("Paxos msg received ++++++++++ "+msg);
					if(state != PLeaderState.DORMANT){
						if(msg.getType() == PaxosMsgType.PREPARE)
						{
							handlePrepareMessage(msg);
						}
						else if(msg.getType() == PaxosMsgType.ACK)
						{
							handleAckMessage(msg);
						}
						else if(msg.getType() == PaxosMsgType.ACCEPT && state != PLeaderState.INIT)
						{ 
							handlePaxosAcceptMessage(msg);
						}
						else if(msg.getType() == PaxosMsgType.DECIDE && state != PLeaderState.INIT)
						{
							//	System.out.println("DECIDE MSG from "+msg.getSource().getHost()+":"+msg.getSource().getPort());
							handlePaxosDecideMessage(msg);
						}
						else{
							AddLogEntry("Paxos Msg :: Not falling in any case "+msg+". Ignoring silently");
						}
					}
					else{
						pendingRequests.add(msgwrap);
					}
				}

				if(msgwrap.getmessageclass() == TwoPCMsg.class )
				{
					TwoPCMsg msg = (TwoPCMsg) msgwrap.getDeSerializedInnerMessage();
					if(state == PLeaderState.ACTIVE)
					{
						if(msg.getMsgType() == TwoPCMsgType.INFO)
						{
							twoPhaseCoordinator.ProcessInfoMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.PREPARE)
						{	
							twoPhaseCoordinator.ProcessPrepareMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.RELEASE)
						{	
							ProcessReleaseResourceMessage(msg);
						}
						else if(msg.getMsgType() == TwoPCMsgType.COMMIT){
							if(msg.isTwoPC()){
								twoPhaseCoordinator.ProcessCommitMessage(msg);
							}
							else{
								handleTwoPCCommitMessage(msg);
							}
						}
						else if(msg.getMsgType() == TwoPCMsgType.ABORT){
							if(msg.isTwoPC()){
								twoPhaseCoordinator.ProcessAbortMessage(msg);
							}
							else{
								handleTwoPCAbortMessage(msg);
							}
						}
						else{
							AddLogEntry("TwoPC msg: Not falling in any case "+msg+". Ignoring silently");
						}
					}
					else{
						pendingRequests.add(msgwrap);
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else{
			//FIX ME
		}
	}



	/**
	 * Method to get the Last Paxos Log position number
	 * @return
	 */
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

	/**
	 * Method to request replay log to Leader
	 * @param logPosition
	 * @param dest
	 */
	private void requestReplayLog(int logPosition, NodeProto dest)
	{
		ReplayMsg msg = new ReplayMsg(nodeAddress, ReplayMsgType.REQEUST);
		msg.setLastLogPosition(logPosition);
		sendReplayLogMsg(dest, msg);
	}

	/**
	 * Method to send ReplayLogMsg to participant/Leader
	 * @param dest
	 * @param message
	 */
	private void sendReplayLogMsg(NodeProto dest, ReplayMsg message)
	{
		this.AddLogEntry("Sent "+message, Level.INFO);

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		metaDataSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();
	}

	/**
	 * Method used to process ReplayLogResponse Message
	 * @param msg
	 */
	private synchronized void processReplayLogResponseMsg(ReplayMsg msg)
	{
		AddLogEntry("Handling response for replay msg", Level.INFO);
		ArrayList<ReplayLogEntry> logEntries = msg.getAllReplayEntries();
		int lastLogPos = getLastPaxosInstanceNumber();
		boolean validFlag = true;
		int size = logEntries.size();
		if(size == 0){
			this.state = PLeaderState.ACTIVE;
			this.AddLogEntry("No entries found to replay. Changed to ACTIVE state");
			sendReplayAckToLeader();
		}
		else
		{
			for(int i=0;i<size;i++)
			{
				ReplayLogEntry logEntry = logEntries.get(i);
				if(i == 0)
				{
					int curLogPos = logEntry.getLogPosition();
					if(curLogPos < lastLogPos){
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
				if(waitingRequest != null)
				{
					this.AddLogEntry("Replayed log. Forwarding request to Leader", Level.INFO);
					forwardClientRequestToLeader(waitingRequest);
				}
				else{
					this.AddLogEntry("Replayed all log entries. Start serving traffic ", Level.INFO);
					this.state = PLeaderState.ACTIVE;
					sendReplayAckToLeader();
				}
			}
			else{
				this.AddLogEntry("Haven't received proper log entries", Level.FINE);
			}

		}
	}

	/**
	 * Method to replay individual log entry
	 * @param logEntry
	 */
	private void replayLog(ReplayLogEntry logEntry)
	{	
		writeToPaxLogFile(logEntry.getLogPosition(), logEntry.getOperation(), logEntry.getLogEntry());
		if(logEntry.getOperation().equalsIgnoreCase("COMMITED"))
			localResource.WriteResource(logEntry.getLogEntry());
	}

	/**
	 * Method to send ReplayAck to Leader
	 */
	private void sendReplayAckToLeader()
	{
		ReplayMsg msg = new ReplayMsg(nodeAddress, ReplayMsgType.ACK);
		this.AddLogEntry("Sent "+msg, Level.INFO);
		//ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		//pushSocket.connect("tcp://"+leaderAddress.getHost()+":"+leaderAddress.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		leaderSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();
	}

	/**
	 * 
	 */
	/*private void sendReplayLogResponseToLeader()
	{
		ReplayMsg msg = new ReplayMsg(nodeAddress, ReplayMsgType.RESPONSE);
		this.AddLogEntry("Sent "+msg, Level.INFO);
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+leaderAddress.getHost()+":"+leaderAddress.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
	}*/

	/*
	 * Method to process ReplayAck message from replica.
	 */
	private synchronized void processReplayAck(ReplayMsg msg)
	{
		if(pendingReplayReplicas.contains(msg.getSource()))
		{
			pendingReplayReplicas.remove(msg.getSource());
			if(pendingReplayReplicas.size() == 0){
				state = PLeaderState.ACTIVE;
				this.AddLogEntry("Status changed to ACTIVE as there are no more replica's waiting to replay");
				while(!pendingRequests.isEmpty())
					handleIncomingMessage(pendingRequests.remove(0));
				this.AddLogEntry("Done with Pending Requests. Start serving traffic", Level.FINE);
			}
		}
		else{
			this.AddLogEntry("Replay Ack not expected from this replica");
		}
	}

	/**
	 * Method to forward pending request to Leader
	 * @param waitingRequest
	 */
	private void forwardClientRequestToLeader(ClientOpMsg waitingRequest)
	{
		this.AddLogEntry("Sent "+waitingRequest);
		//ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		//pushSocket.connect("tcp://"+leaderAddress.getHost()+":"+leaderAddress.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(waitingRequest), waitingRequest.getClass());
		leaderSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();
	}

	/**
	 * Method to process incoming ReplayLog Request message.
	 * Applies only to Leader
	 * @param msg
	 */
	private void processReplayLogRequestMsg(ReplayMsg msg){
		AddLogEntry("Handling request for replay msg "+msg+"from "+msg.getSource().getHost()+":"+msg.getSource().getPort());
		this.state = PLeaderState.DORMANT;
		pendingReplayReplicas.add(msg.getSource());
		SendReplayLog(msg.getLastLogPosition(), msg.getSource());
	}

	/**
	 * Method to send ReplayLog to the requested replica
	 * @param logPosition
	 * @param dest
	 */
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
			this.AddLogEntry("Prepared replay log entries. Sending response to "+dest.getHost()+":"+dest.getPort(), Level.INFO);
			sendReplayLogMsg(dest, msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendPaxosMsgRequestingAcceptors()
	{
		this.AddLogEntry("Sending msg to MDS reqeusting Acceptors", Level.INFO);
		PaxosDetailsMsg msg = new PaxosDetailsMsg(nodeAddress, shard, PaxosDetailsMsgType.ACCEPTORS);
		sendMsgToMDS(metadataService ,msg);
	}

	private void sendPaxosMsgRequestingLeader()
	{

		if(leaderAddress == null){
			this.AddLogEntry("Sending msg to MDS reqeusting leader", Level.INFO);
			PaxosDetailsMsg msg = new PaxosDetailsMsg(nodeAddress, shard, PaxosDetailsMsgType.LEADER);
			sendMsgToMDS(metadataService ,msg);
		}
		else{
			this.AddLogEntry("I know the leader address. No need for replay log", Level.INFO);
			state = PLeaderState.ACTIVE;
		}

	}

	/**
	 * Method to handle Paxos Details msg rgdn Acceptors info from MDS
	 * @param msg
	 */
	private void handlePaxosDetailsAcceptorsMsg(PaxosDetailsMsg msg)
	{
		acceptors = new ArrayList<NodeProto>();
		ArrayList<NodeProto> acceptorList = msg.getReplicas();
		for(NodeProto node: acceptorList)
		{
			if(node != nodeAddress){
				acceptors.add(node);
				ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
				pushSocket.connect("tcp://"+node.getHost()+":"+node.getPort());
				addressToSocketMap.put(node, pushSocket);
			}
		}
		acceptorsCount = acceptors.size();
		AddLogEntry("Received Metadata (Acceptors) response from MDS \n");
		StringBuffer buffer = new StringBuffer();
		buffer.append("==============================================================\n");
		buffer.append("List of Acceptors : ");
		for(NodeProto nodeProto : acceptors)
			buffer.append(nodeProto.getHost()+":"+nodeProto.getPort()+", ");
		buffer.append("\n==============================================================");
		AddLogEntry(buffer.toString()+"\n");
	}

	/**
	 * Method to handle Paxos Details msg rgdn Leader info from MDS
	 * @param msg
	 */
	private synchronized void handlePaxosDetailsLeaderMsg(PaxosDetailsMsg msg)
	{
		AddLogEntry("Received Metadata (Leader) response from MDS \n");
		leaderAddress = msg.getShardLeader();

		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+leaderAddress.getHost()+":"+leaderAddress.getPort());
		leaderSocket = pushSocket;
		if(leaderAddress != null)
		{
			this.AddLogEntry("Leader availabe "+leaderAddress.getHost()+":"+leaderAddress.getPort()+"\n", Level.INFO);
			int logPosition = getLastPaxosInstanceNumber();
			logCounter = logPosition;
			if(leaderAddress.equals(nodeAddress)){
				isLeader= true;
				//	twoPhaseCoordinator = new TwoPC(shard, nodeAddress, context, isNew);
				this.state = PLeaderState.ACTIVE;
				this.AddLogEntry("I am the leader as given by MDS\n" , Level.INFO);
			}
			else{
				if(!clearLog){
					AddLogEntry("Last log position "+logPosition, Level.FINE);
					this.AddLogEntry("Requesting replay log to Leader "+leaderAddress.getHost()+":"+leaderAddress.getPort()+"\n", Level.INFO);
					requestReplayLog(logPosition, leaderAddress);
				}
				else{
					this.state = PLeaderState.ACTIVE;
					this.AddLogEntry("Starting logging from scratch\n", Level.INFO);
				}
			}
		}
		else{
			if(waitingRequest != null)
			{
				this.AddLogEntry("Leader not set. Initiating Prepare phase for the waiting Request\n", Level.INFO);
				if(waitingRequest.getMsgType() == ClientOPMsgType.READ)
					initiatePreparePhase(waitingRequest.getTransaction().getTransactionID(), waitingRequest.getTransaction().getReadSet());
				else
					initiatePreparePhase(waitingRequest.getTransaction().getTransactionID(), waitingRequest.getTransaction().getWriteSet());

			}
			else{

				this.AddLogEntry("Leader not set. No waiting Requests found. Initiaing Prepare phase with dummy values\n", Level.INFO);
				initiatePreparePhase("-1", null);
			}
		}
	}


	/**
	 * Method to send PaxosDetails message to MDS
	 * @param dest
	 * @param message
	 */
	private void sendMsgToMDS(NodeProto dest, PaxosDetailsMsg message)
	{
		this.AddLogEntry("Sent :: "+message+"to "+dest.getHost()+":"+dest.getPort()+"\n", Level.INFO);
		//	ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		//pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		metaDataSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();
	}

	/**
	 * Method to send Leader message to MDS
	 * @param dest
	 * @param message
	 */
	private void sendMsgToMDS(NodeProto dest, LeaderMsg message)
	{
		this.AddLogEntry("Sent :: "+message+"to "+dest.getHost()+":"+dest.getPort()+"\n", Level.INFO);
		//ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		//pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		metaDataSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();
	}


	/**
	 * Method to send Msg to MDS informing about leadership
	 */
	private void announceMDSAboutLeaderShip()
	{
		LeaderMsg msg = new LeaderMsg(nodeAddress, LeaderMsgType.RESPONSE, shard);
		msg.setLeader();
		sendMsgToMDS(metadataService, msg);
	}


	/**
	 * Method to send ClientOpMsg to transactional client
	 * @param message
	 * @param dest
	 */
	private void SendClientMessage(ClientOpMsg message, NodeProto dest)
	{
		this.AddLogEntry("Sent "+message, Level.INFO);

		ZMQ.Socket pushSocket = null;
		if(addressToSocketMap.containsKey(dest))
		{
			pushSocket = addressToSocketMap.get(dest);
		}
		else{
			pushSocket = context.socket(ZMQ.PUSH);
			pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
			addressToSocketMap.put(dest, pushSocket);
		}
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );

	}


	/**
	 * Method to send Paxos Msg to the designated replica/Leader
	 * @param dest
	 * @param msg
	 */
	private void sendPaxosMsg(NodeProto dest, PaxosMsg msg){
		//System.out.println("Sent " + msg+" from "+nodeAddress.getHost()+":"+nodeAddress.getPort() +" to "+dest.getHost()+":"+dest.getPort()+"\n");
		//this.AddLogEntry("Sent "+msg+"\n");
		ZMQ.Socket pushSocket = null;
		if(addressToSocketMap.containsKey(dest))
			pushSocket = addressToSocketMap.get(dest);
		else{
			pushSocket = context.socket(ZMQ.PUSH);
			pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
			addressToSocketMap.put(dest, pushSocket);
		}
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();	
	}

	/**
	 * Method to handle incoming client read message
	 * @param message
	 * @throws IOException
	 */
	private synchronized void handleClientReadMessage(ClientOpMsg message) throws IOException
	{
		if(isLeader){
			AddLogEntry("Handling process client read msg "+message);
			NodeProto transClient = message.getSource();
			TransactionProto trans = message.getTransaction();

			boolean isReadLock = true;
			if(!localResource.ifReadResourceExists(trans.getReadSet()))
				isReadLock = false;
			if(isReadLock){
				for(ElementProto element : trans.getReadSet().getElementsList())
				{
					if(!lockTable.getReadLock(element.getRow(), trans.getTransactionID()))
						isReadLock = false;
				}
			}

			AddLogEntry("Acquired all read locks."+isReadLock, Level.FINE);
			ClientOpMsg read_response = null;
			if(isReadLock){
				ElementsSetProto readValues = localResource.ReadResource(trans.getReadSet());

				TransactionProto transaction = TransactionProto.newBuilder()
						.setTransactionID(trans.getTransactionID())
						.setTransactionStatus(TransactionStatusProto.ACTIVE)
						.setReadSet(readValues)
						.build();
				uidTransMap.put(trans.getTransactionID(), new TransactionSource(trans, transClient, TwoPCMsgType.INFO));
				read_response = new ClientOpMsg(nodeAddress, transaction, ClientOPMsgType.READ_RESPONSE, true);
			}
			else{
				read_response = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.READ_RESPONSE, false);
				for(ElementProto element : trans.getReadSet().getElementsList())
				{
					lockTable.releaseReadLock(element.getRow(), trans.getTransactionID(), false);
				}
			}

			SendClientMessage(read_response, transClient);
			//			System.out.println("Sent Read Data for UID - "+trans.getTransactionID());		
			AddLogEntry("Sent Read Data for UID - "+trans.getTransactionID()+" "+read_response);
		}
		else{
			waitingRequest = message;
			sendPaxosMsgRequestingLeader();
		}
	}

	/**
	 * Method to process Client write message
	 * @param msg, ClientOpMsg
	 */
	private void handleClientWriteMessage(ClientOpMsg msg)
	{	
		AddLogEntry("Handling client write msg "+msg);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();
			boolean isWriteLock = true;
			for(ElementProto element : trans.getWriteSet().getElementsList())
			{
				if(!lockTable.getWriteLock(element.getRow(), trans.getTransactionID()))
				{
					isWriteLock = false;
				}
			}
			AddLogEntry("IsWritelock Acquired "+isWriteLock, Level.FINE);
			if(isWriteLock){
				int newLogPosition = logCounter++;
				AddLogEntry("Initiating new PaxosInstance for Log Position "+newLogPosition);
				uidTologPositionMap.put("P"+trans.getTransactionID(), newLogPosition);
				logPostionToUIDMap.put(newLogPosition, "P"+trans.getTransactionID());
				PaxosInstance paxInstance = new PaxosInstance("P"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				uidTransMap.put( "P"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.PREPARE));
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				paxInstance.addtoAcceptList(nodeAddress);
				pendingPaxosInstances.add(newLogPosition);
				uidPaxosInstanceMap.put("P"+trans.getTransactionID(), paxInstance);
				logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				AddLogEntry("Sending accept msgs to all acceptors \n");
				PaxosMsg message = new PaxosMsg(nodeAddress, "P"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				sendAcceptMsg(message);
			}
			else{
				AddLogEntry("Not able to acquire locks. Aborting the transaction");
				lockTable.releaseLocksOfAborted(trans);
				//Fix me
				TwoPCMsg message = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
		else{
			AddLogEntry("I am not the leader. Request Leader info from MDS", Level.INFO);
			waitingRequest = msg;
			sendPaxosMsgRequestingLeader();
		}
	}

	/**
	 * Method Accept messages to all replicas
	 * @param msg
	 */
	private void sendAcceptMsg(PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				AddLogEntry("Sending ACCEPT message "+msg+"to "+node.getHost()+":"+node.getPort()+"\n");
				sendPaxosMsg(node, msg);
			}
		}
	}


	/**
	 * Method to initiate PAXOS PREPARE phase for a new log position
	 */
	private void initiatePreparePhase()
	{
		int newLogPosition = logCounter;
		PaxosInstance paxInstance = new PaxosInstance("-1", ballotNo);
		logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
		PaxosMsg msg = new PaxosMsg(nodeAddress, "-1", PaxosMsgType.PREPARE, ballotNo);
		msg.setLogPositionNumber(newLogPosition);
		SendPrepareMessage(msg);
	}

	/**
	 * Method to initiate PAXOS PREPARE phase for a new log position
	 * @param uid
	 * @param acceptedValue
	 */
	private synchronized void initiatePreparePhase(String uid, ElementsSetProto acceptedValue)
	{
		int newLogPosition = logCounter;
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo, acceptedValue);
		logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
		PaxosMsg msg = new PaxosMsg(nodeAddress, uid, PaxosMsgType.PREPARE, ballotNo);
		msg.setLogPositionNumber(newLogPosition);
		SendPrepareMessage(msg);
	}

	/**
	 * Process release resource message
	 * @param message, ClientOpMsg
	 */
	private synchronized void handleClientReleaseResourceMsg(ClientOpMsg message)
	{
		AddLogEntry("Received client release resource msg "+message);
		TransactionProto trans = message.getTransaction();
		//FIX ME: check if commited or aborted trans
		if(trans.getReadSet() != null){
			if(trans.getTransactionStatus() == TransactionStatusProto.COMMITTED)
				lockTable.releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), true);
			else
				lockTable.releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), false);
		}
		else{
			if(trans.getTransactionStatus() == TransactionStatusProto.COMMITTED)
				lockTable.releaseWriteLocks(trans.getWriteSet(), trans.getTransactionID(), true);
			else
				lockTable.releaseWriteLocks(trans.getWriteSet(), trans.getTransactionID(), false);
		}
		AddLogEntry("Released all resources. No ack sent");
	}

	/**
	 * Process release resource message
	 * @param message, ClientOpMsg
	 */
	private synchronized void handleClientReleaseReadSet(ClientOpMsg message)
	{
		AddLogEntry("Received client release resource msg "+message);
		TransactionProto trans = message.getTransaction();
		//FIX ME: check if commited or aborted trans
		lockTable.releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), true);
		AddLogEntry("Released all resources. No ack sent");
	}


	/**
	 * Method to send Prepare message
	 * @param message
	 */
	private void SendPrepareMessage(PaxosMsg message)
	{
		SendMessageToAcceptors(message);
	}

	/**
	 * Method to Send Paxos message to all replicas/acceptors
	 * @param msg
	 */
	private void SendMessageToAcceptors(PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!nodeAddress.equals(node))
				sendPaxosMsg(node, msg);
		}
	}


	/**
	 * Process release resource message from TwoPc
	 * @param message, ClientOpMsg
	 */
	private synchronized void ProcessReleaseResourceMessage(TwoPCMsg message)
	{
		AddLogEntry("Received release resource(read set) msg "+message);
		TransactionProto trans = message.getTransaction();
		lockTable.releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), true);
		AddLogEntry("Released all resources. No ack sent");
	}


	/**
	 * Method to process PAXOS PREPARE message
	 * @param msg
	 */
	private synchronized void handlePrepareMessage(PaxosMsg msg)
	{
		if(logPositionToPaxInstanceMap.contains(msg.getLogPositionNumber()))
		{
			this.AddLogEntry("Already seen another paxos instance for the same log position ");
			PaxosInstance paxInstance = logPositionToPaxInstanceMap.get(msg.getLogPositionNumber());
			if( (msg.getBallotNumber().compareTo(paxInstance.getBallotNumber()) >= 0))
			{
				AddLogEntry("Sending ACK to "+ msg.getSource().getHost()+":"+msg.getSource().getPort()+" with my prev stored values (received Bal >= my prev BN)");
				PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID() , PaxosMsgType.ACK, msg.getBallotNumber(), paxInstance.getBallotNumber(), paxInstance.getAcceptedValue());
				message.setLogPositionNumber(msg.getLogPositionNumber());
				sendPaxosMsg(msg.getSource(), message);
			}
			else{
				this.AddLogEntry("Ignoring received PREPARE msg from "+ msg.getSource().getHost()+":"+msg.getSource().getPort()+" as my prev stored BN ("+paxInstance.getBallotNumber()+" > received Bal "+msg.getBallotNumber());
			}
		}
		else{
			this.AddLogEntry("Received PREPARE msg for this log position "+msg.getLogPositionNumber()+" for the first time", Level.INFO);
			this.AddLogEntry("Sending ACK to "+ msg.getSource().getHost()+":"+msg.getSource().getPort()+" with null values ", Level.INFO);
			PaxosInstance paxInstance = new PaxosInstance(msg.getUID(), msg.getBallotNumber());
			logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
			PaxosMsg message = new PaxosMsg(nodeAddress, msg.getUID() , PaxosMsgType.ACK, msg.getBallotNumber(), null, null);
			message.setLogPositionNumber(msg.getLogPositionNumber());
			sendPaxosMsg(msg.getSource(), message);
		}
	}

	/**
	 * Method to process ACK messages from replicas
	 * @param msg
	 */
	private synchronized void handleAckMessage(PaxosMsg msg)
	{
		int logPos = msg.getLogPositionNumber();
		AddLogEntry("Received ACK for position "+logPos );
		if(logPositionToPaxInstanceMap.get(logPos) != null)
		{
			PaxosInstance paxInstance = logPositionToPaxInstanceMap.get(logPos);
			if(msg.getAcceptNo() != null)
			{
				if(paxInstance.getAcceptedNumber() == null || msg.getAcceptNo().compareTo(paxInstance.getAcceptedNumber()) > 0)
				{
					this.AddLogEntry("Updating Paxos instance values( AN and AV) with received value", Level.INFO);
					paxInstance.setAcceptNumber(msg.getAcceptNo());
					paxInstance.setAcceptedValue(msg.getAcceptValue());
				}
				else{
					this.AddLogEntry("Ignoring the accepted value as ballotNo is smaller",Level.INFO );
				}
			}
			paxInstance.acks.add(msg.getSource());
			if(paxInstance.acks.size() > acceptorsCount/2 && !paxInstance.isAcceptSent)
			{
				this.isLeader = true;
				//	twoPhaseCoordinator = new TwoPC(shard, nodeAddress, context, isNew);
				leaderAddress = nodeAddress;
				paxInstance.isAcceptSent= true;
				announceMDSAboutLeaderShip();
				logPositionToPaxInstanceMap.put(logPos, paxInstance);
				if(paxInstance.getAcceptedValue() != null){
					this.AddLogEntry("Reached majority of ACKS. Sending ACCEPT msgs to all acceptors. (Became the LEADER in the process)", Level.INFO);
					PaxosMsg message = new PaxosMsg( nodeAddress , paxInstance.getUID(), PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), paxInstance.getAcceptedValue());
					message.setLogPositionNumber(logPos);
					SendMessageToAcceptors(message);
				}
				else{
					this.AddLogEntry("Reached majority of ACKS. No Accepted Value found. Elected as a leader", Level.INFO);
				}
			}
			else
				logPositionToPaxInstanceMap.put(logPos, paxInstance);
		}
		else{
			this.AddLogEntry("Received ACK msg for a log position for which no PREPARE was sent. Hence silently ignoring the msg", Level.INFO);
		}
	}




	/**
	 * Mehtod to process incoming ACCEPT msg
	 * @param msg
	 */
	private synchronized void handlePaxosAcceptMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		AddLogEntry("\nHandling ACCEPT msg "+msg);

		if(logPostionToUIDMap.containsKey(msg.getLogPositionNumber()))
		{	
			AddLogEntry("Already having paxInstance for the same log Position ", Level.FINE);
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
							AddLogEntry("Reached majority of acceptors, but DECIDE not yet sent. DECIDING and sending DECIDE msg to all replicas");						
							paxInstance.isDecideSent = true;
							paxInstance.addToDecideList(nodeAddress);
							if(pendingPaxosInstances.contains(msg.getLogPositionNumber()))
								pendingPaxosInstances.remove(msg.getLogPositionNumber());

							StringBuffer buffer = new StringBuffer();
							buffer.append("AcceptValue decided to be :: \n");

							for(ElementProto elementProto:  paxInstance.getAcceptedValue().getElementsList())
							{
								String temp = elementProto.getRow()+":";
								for(ColElementProto colElemProto: elementProto.getColsList())
								{
									temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
								}
								buffer.append(temp+"\n");
							}
							AddLogEntry(buffer.toString());
							paxInstance.setCommited();
							uidPaxosInstanceMap.put(uid, paxInstance);
							logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
							initiateDecide(msg);
						}
						else{
							AddLogEntry("Reached majority and already DECIDED. No action taken");
						}
					}
				}
			}
			else{
				AddLogEntry("Already accepted Ballot Number is greater than the received one. My cur Ballot Number: "+paxInstance.getBallotNumber()+", received "+msg.getBallotNumber());
			}
		}
		else{
			AddLogEntry("First time seeing ACCEPT msg. Accepting the value and sending ACCEPT to all");
			PaxosInstance paxInstance = new PaxosInstance(uid, msg.getBallotNumber(), msg.getAcceptValue());
			paxInstance.addtoAcceptList(msg.getSource());
			paxInstance.addtoAcceptList(nodeAddress);
			paxInstance.setTimeStamp(new Date().getTime());

			boolean isWriteLock = true;
			for(ElementProto element : paxInstance.getAcceptedValue().getElementsList())
			{
				if(!lockTable.getWriteLock(element.getRow(), uid.substring(1)))				
					isWriteLock = false;
			}

			if(isWriteLock){
				paxInstance.setAcceptSent();
				uidPaxosInstanceMap.put(uid, paxInstance );

				int newLogPosition = logCounter++;
				AddLogEntry("Initiating paxos instance for log position "+newLogPosition);
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
				int newLogPosition = logCounter++;
				AddLogEntry("Not able to acquire locks. Hence Aborting the transaction");
				writeToPaxLogFile(newLogPosition, "ABORTED", paxInstance.getAcceptedValue());
				logPositionToPaxInstanceMap.put(newLogPosition, dummyInstance);
				TransactionSource tempTransSource = uidTransMap.get(msg.getUID());
				lockTable.releaseLocksOfAborted(tempTransSource.trans);
				//ClientOpMsg message = new ClientOpMsg(nodeAddress, tempTransSource.getTrans(), ClientOPMsgType.ABORT);
				//SendClientMessage(message, tempTransSource.getSource());
				AddLogEntry("Sending client response(ABORT) to "+tempTransSource.getSource()+" for "+uid);
				TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
	}

	/**
	 * Method to iniate Decide phase for a Paxos Instance
	 * @param msg
	 */
	private void initiateDecide(PaxosMsg msg)
	{
		String uid = msg.getUID();
		PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);

		AddLogEntry("PAXOS Reached DECISION in ::::: "+(System.currentTimeMillis() - paxInstance.getTimeStamp()));
		if(uid.startsWith("C")){
			Boolean isWritten = localResource.WriteResource(paxInstance.getAcceptedValue());

			if(isWritten){
				writeToPaxLogFile(uidTologPositionMap.get(uid), "COMMITED", paxInstance.getAcceptedValue());
				AddLogEntry("COMMITED. Releasing the resources for "+uid);
				releaseLocks(paxInstance.getAcceptedValue(), uid.substring(1));
				if(uidTransMap.containsKey(uid.substring(1))){
					ElementsSetProto readSet = uidTransMap.get(uid.substring(1)).trans.getReadSet();
					if(readSet != null)
						lockTable.releaseReadLocks(uidTransMap.get(uid.substring(1)).trans.getReadSet(), uid.substring(1), true);
					//FIX ME: send DECIDE msg to participants
				}
				AddLogEntry("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY for a COMMIT transaction "+uid+"\n");
				PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
				message.setLogPositionNumber(msg.getLogPositionNumber());
				sendDecideMsg(message);
				if(isLeader)
				{
					AddLogEntry("Decided on a value(Commited). Sending response to TPC "+uid);
					sendPaxosInstanceResponse(msg);
				}
			}
			else{
				//send client response
				///FIX me . send to TPC
				writeToPaxLogFile(uidTologPositionMap.get(uid), "ABORTED", paxInstance.getAcceptedValue());
				if(isLeader){
					TransactionSource tempTransSource = uidTransMap.get(msg.getUID());					
					lockTable.releaseLocksOfAborted(tempTransSource.trans);
					TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
					AddLogEntry("Sending client response(ABORT)"+message+" to "+tempTransSource.getSource() );
					SendTwoPCMessage(message, msg.getSource());
				}
			}
		}
		else if(uid.startsWith("P")){//Pax instance for PREPARE phase. Just obtained read locks
			AddLogEntry("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY for a PREPARE transaction "+uid+"\n");
			//FIX ME: send DECIDE msg to participants
			writeToPaxLogFile(uidTologPositionMap.get(uid), "PREPARED", paxInstance.getAcceptedValue());
			PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
			message.setLogPositionNumber(msg.getLogPositionNumber());
			sendDecideMsg(message);
			if(isLeader){
				AddLogEntry("Done with PREPARE phase. Sending response to TPC ");
				sendPaxosInstanceResponse(msg);
			}
		}
		else if(uid.startsWith("A"))
		{
			//FIX ME: send DECIDE msg to participants
			AddLogEntry("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY for a ABORT transaction "+uid+"\n");
			lockTable.releaseLocksOfAborted(uidTransMap.get(uid).trans);
			PaxosMsg message = new PaxosMsg(nodeAddress, uid, PaxosMsgType.DECIDE, msg.getBallotNumber(), msg.getAcceptValue());
			message.setLogPositionNumber(msg.getLogPositionNumber());
			sendDecideMsg(message);
			if(isLeader){
				AddLogEntry("Done with ABORT. Sending response to TPC ");
				sendPaxosInstanceResponse(msg);
			}
		}
	}

	/**
	 * Method to process Decide Message
	 * @param msg
	 */
	private synchronized void handlePaxosDecideMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		AddLogEntry("\nHandling DECIDE msg "+msg+"\n");
		if(logPositionToPaxInstanceMap.containsKey(msg.getLogPositionNumber()))
		{
			PaxosInstance paxInstance = logPositionToPaxInstanceMap.get(msg.getLogPositionNumber());
			AddLogEntry("Paxos Isntance "+paxInstance, Level.FINE);
			int prevCount = paxInstance.decides.size();
			paxInstance.decides.add(msg.getSource());
			/*	System.out.print("List of decides <<<<< ");
			for(NodeProto nodeProto: paxInstance.decides)
			{
				System.out.print(nodeProto.getHost()+":"+nodeProto.getPort()+";");
			}
			System.out.println(" >>>>>>>>>>>>>>>>>>>>");*/

			int newCount = paxInstance.decides.size();
			if(prevCount < newCount ){
				if(!paxInstance.isDecideSent)
				{
					paxInstance.setTimeStamp(new Date().getTime());
					paxInstance.isDecideSent = true;
					paxInstance.addToDecideList(nodeAddress);
					StringBuffer buffer = new StringBuffer();
					buffer.append("AcceptValue decided to be :: \n");

					for(ElementProto elementProto:  paxInstance.getAcceptedValue().getElementsList())
					{
						String temp = elementProto.getRow()+":";
						for(ColElementProto colElemProto: elementProto.getColsList())
						{
							temp += colElemProto.getCol()+","+colElemProto.getValue()+";";
						}
						buffer.append(temp+"\n");
					}
					AddLogEntry(buffer.toString());
					paxInstance.setCommited();
					uidPaxosInstanceMap.put(uid, paxInstance);
					logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
					pendingPaxosInstances.remove(msg.getLogPositionNumber());
					initiateDecide(msg);
				}
				else{
					AddLogEntry("Reached majority and already DECIDED. No action taken");
				}
			}
			else{
				AddLogEntry("Already seen DECIDE from the same acceptor");
			}
		}
		else{
			AddLogEntry("First time seeing DECIDE msg. DECIDING upon the value and sending DECIDE to all");
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

	/**
	 * Method to send Decide message to all replicas 
	 * @param msg
	 */
	private void sendDecideMsg(PaxosMsg msg)
	{
		for(NodeProto node: acceptors)
		{
			if(!node.equals(nodeAddress))
			{
				AddLogEntry("Sending DECIDE msg "+msg+" to "+node.getHost()+":"+node.getPort()+"\n");
				sendPaxosMsg(node, msg);
			}
		}
	}


	/**
	 * Method to release locks for the given readset or writeset
	 * @param elementsSetProto
	 * @param uid
	 */
	private void releaseLocks(ElementsSetProto elementsSetProto, String uid)
	{
		for(ElementProto element : elementsSetProto.getElementsList())
		{
			lockTable.releaseWriteLock(element.getRow(), uid, true);
		}
		AddLogEntry("Released all resources for "+ uid+". No ack sent");

	}

	/**
	 * Method to send Paxos Instance response to TPC
	 * @param msg
	 */
	private void sendPaxosInstanceResponse(PaxosMsg msg)
	{
		//send response to TPC
		TransactionSource tempTransSource = uidTransMap.get(msg.getUID());

		if(tempTransSource.getType() == TwoPCMsgType.PREPARE)
		{	
			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.PREPARE, true);
			AddLogEntry("Sent PREPARE_ACK msg "+message+" to "+tempTransSource.getSource().getHost()+":"+tempTransSource.getSource().getPort());
			SendTwoPCMessage(message, tempTransSource.getSource());

		}
		else if(tempTransSource.getType() == TwoPCMsgType.COMMIT){
			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.COMMIT, true);
			AddLogEntry("Sent COMMIT_ACK msg "+message+" to "+tempTransSource.getSource().getHost()+":"+tempTransSource.getSource().getPort());
			SendTwoPCMessage(message, tempTransSource.getSource());
		}

		else if(tempTransSource.getType() == TwoPCMsgType.ABORT){
			TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
			AddLogEntry("Sent ABORT msg "+message+" to "+tempTransSource.getSource().getHost()+":"+tempTransSource.getSource().getPort());
			SendTwoPCMessage(message, tempTransSource.getSource());
		}
	}


	/**
	 * Method to process TwoPC Commit message
	 * @param msg
	 */
	private void handleTwoPCCommitMessage(TwoPCMsg msg)
	{
		AddLogEntry("Received TwoPC Commit Message "+msg);
		if(isLeader)
		{
			TransactionProto trans = msg.getTransaction();
			//AddLogEntry("New commit trans from TPC :: "+msg);
			boolean isWriteLock = true;
			for(ElementProto element : trans.getWriteSet().getElementsList())
			{
				//System.out.println("Trying to acquire lock for trans "+trans.getTransactionID());
				if(!lockTable.getWriteLock(element.getRow(), trans.getTransactionID()))
				{
					isWriteLock = false;
				}
			}
			AddLogEntry("IsWritelock Acquired "+isWriteLock, Level.FINE);
			if(isWriteLock){
				//Already leader. Send Accept right Away
				int newLogPosition = logCounter++;
				AddLogEntry("Initiating new Paoxs Instance for a Commit transaction at log position "+newLogPosition);
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
				AddLogEntry("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			}
			else{
				AddLogEntry("Not able to acquire locks. Hence, aborting the transaction");
				//Fix me
				lockTable.releaseLocksOfAborted(trans);
				TwoPCMsg message = new TwoPCMsg(nodeAddress, trans, TwoPCMsgType.ABORT);
				SendTwoPCMessage(message, msg.getSource());
			}
		}
		else{
			//FIX ME
		}
	}

	/**
	 * Method to process TwoPC Abort message
	 * @param msg
	 */
	private synchronized void handleTwoPCAbortMessage(TwoPCMsg msg)
	{
		AddLogEntry("Received TwoPC Abort Message "+msg);
		if(isLeader)
		{	
			TransactionProto trans = msg.getTransaction();
			String uid = trans.getTransactionID();
			if(!uidPaxosInstanceMap.contains("A"+uid)){
				if(pendingPaxosInstances.contains(uidTologPositionMap.get("P"+uid)))
				{
					AddLogEntry("Aborting paxInstance of PREPARE txn for the received Abort trans ");
					pendingPaxosInstances.remove(uidTologPositionMap.get("P"+uid));
				}				
				int newLogPosition = logCounter++;
				AddLogEntry("New Abort Transaction "+msg +" for log position "+newLogPosition);
				uidTologPositionMap.put("A"+trans.getTransactionID(), newLogPosition);
				logPostionToUIDMap.put(newLogPosition, "A"+trans.getTransactionID());
				writeToPaxLogFile(newLogPosition, "ABORTED", trans.getWriteSet());
				PaxosInstance paxInstance = new PaxosInstance("A"+trans.getTransactionID(), ballotNo, trans.getWriteSet());
				uidTransMap.put( "A"+trans.getTransactionID(), new TransactionSource(trans,msg.getSource(), TwoPCMsgType.ABORT));
				lockTable.releaseLocksOfAborted(trans);
				paxInstance.setAcceptSent();
				paxInstance.setTimeStamp(new Date().getTime());
				pendingPaxosInstances.add(newLogPosition);
				paxInstance.addtoAcceptList(nodeAddress);

				uidPaxosInstanceMap.put("A"+trans.getTransactionID(), paxInstance);
				logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				PaxosMsg message = new PaxosMsg(nodeAddress, "A"+trans.getTransactionID(),PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), trans.getWriteSet());
				message.setLogPositionNumber(newLogPosition);
				AddLogEntry("Sending accept msgs to all acceptors ");
				sendAcceptMsg(message);
			}
			else{
				AddLogEntry("Already Trans aborted. No action taken");
			}
		}
		else{
			//yet to fill in
		}
	}


	/**
	 * Method to send TwoPC message to TPC
	 * @param message
	 * @param dest
	 */
	private void SendTwoPCMessage(TwoPCMsg message, NodeProto dest)
	{
		ZMQ.Socket pushSocket = null;
		if(addressToSocketMap.containsKey(dest))
			pushSocket = addressToSocketMap.get(dest);
		else{
			pushSocket = context.socket(ZMQ.PUSH);
			pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
			addressToSocketMap.put(dest, pushSocket);
		}

		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		//pushSocket.close();
	}


	/**
	 * Method used to append content to Transactional Log file
	 * @param counter
	 * @param type
	 * @param acceptedValue
	 */
	private void writeToPaxLogFile(int counter, String type, ElementsSetProto acceptedValue){

		//FIX ME:
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
			e.printStackTrace();
		}
	}


	public static void main(String args[]) throws IOException
	{
		if(args.length <= 2)
			throw new IllegalArgumentException("Usage: PAcceptor <ShardID> <nodeId> true/false(clear log file or no)");
		boolean isNew = Boolean.parseBoolean(args[2]);
		PaxosAcceptor acceptor = new PaxosAcceptor(args[0], args[1], isNew);
		new Thread(acceptor).start();
		acceptor.executeDaemon();
	}

}
