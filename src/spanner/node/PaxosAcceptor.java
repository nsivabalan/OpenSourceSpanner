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
import java.util.concurrent.atomic.AtomicInteger;
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
	boolean isLeader ;
	String shard ;
	PLeaderState state ;
	ArrayList<NodeProto> acceptors;
	LockTable lockTable = null;
	private static boolean clearLog ;
	private static File PAXOSLOG = null;
	int acceptorsCount = 0;

	private ReplicationManager replicationManager = null;
	public HashMap<String, Integer> uidTologPositionMap = null; 
	public HashMap<Integer, String> logPostionToUIDMap = null;
	public HashMap<String, TransactionSource> uidTransMap = null;
	private ClientOpMsg waitingRequest = null;
	public PaxosInstance dummyInstance = null;
	private ArrayList<MessageWrapper> pendingRequests = null;
	private HashSet<NodeProto> pendingReplayReplicas = null;
	TwoPC twoPhaseCoordinator = null;
	public AtomicInteger logCounter = new AtomicInteger(0);
	RandomAccessFile logRAF = null;
//	private static ResourceHM localResource = null;
	private static Resource localResource = null;
	private static FileHandler logFile = null;

	public PaxosAcceptor(String shard, String nodeId, boolean isNew) throws IOException
	{
		super(nodeId, isNew);
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
		AddLogEntry("Connected @ "+nodeAddress.getHost()+":"+nodeAddress.getPort(), Level.FINE);
		String[] mds = Common.getProperty("mds").split(":");
		metadataService = NodeProto.newBuilder().setHost(mds[0]).setPort(Integer.parseInt(mds[1])).build();
		//FIX ME: check what needs to be passed in as constructor
		lockTable = new LockTable(nodeId, isNew);
		uidTransMap = new HashMap<String, TransactionSource>();
		//localResource = new ResourceHM(this.LOGGER);
		localResource = new Resource(this.LOGGER);
		dummyInstance = new PaxosInstance(null,  null);
		state = PLeaderState.INIT;
		myId = Integer.parseInt(nodeId);
		uidTologPositionMap = new HashMap<String, Integer>();
		logPostionToUIDMap = new HashMap<Integer, String>();
		pendingRequests = new ArrayList<MessageWrapper>();
		pendingReplayReplicas = new HashSet<NodeProto>();
		this.clearLog = isNew;
		replicationManager = new ReplicationManager(this);
		new Thread(replicationManager).start();
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
	
	public void handleAbortsForReplicationInstance(String uid, int logPos, ElementsSetProto acceptedValue)
	{
			AddLogEntry("Total ACCEPTs haven't reached majority. Hence aborting the trans "+uid);
			if(uid.startsWith("P")){
				AddLogEntry("PREPARE txn "+uid+" hasn't reached majority. Releasing all locks obtained");
				releaseLocks(acceptedValue, uid);
			}
			else if(uid.startsWith("C")){
				AddLogEntry("COMMIT txn "+uid +" hasn't reached majority. Hence aborting the transaction");
				releaseLocks(acceptedValue, uid);
				//FIX ME: check if paxos leader should respond to the TPC or rely on TPC timeouts
				TwoPCMsg message = new TwoPCMsg(nodeAddress, uidTransMap.get(uid).getTrans(), TwoPCMsgType.ABORT, true);
				SendTwoPCMessage(message, uidTransMap.get(uid).getSource());
			}
			else if(uid.startsWith("A"))
			{
				AddLogEntry("ABORT txn "+uid+" hasn't reached majority. No action taken");
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
						replicationManager.handleIncomingMessage(msg, state);
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
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
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
				logCounter.set(lastEntry.getLogPosition() +1);
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
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+leaderAddress.getHost()+":"+leaderAddress.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
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
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+leaderAddress.getHost()+":"+leaderAddress.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(waitingRequest), waitingRequest.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
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
			if(node != nodeAddress)
				acceptors.add(node);
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
		if(leaderAddress != null)
		{
			this.AddLogEntry("Leader availabe "+leaderAddress.getHost()+":"+leaderAddress.getPort()+"\n", Level.INFO);
			int logPosition = getLastPaxosInstanceNumber();
			logCounter.set(logPosition);
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
				this.AddLogEntry("Leader not set. Initiating Leader Election for the waiting Request\n", Level.INFO);
				if(waitingRequest.getMsgType() == ClientOPMsgType.READ)
					replicationManager.initiateLeaderElection(waitingRequest.getTransaction().getTransactionID(), waitingRequest.getTransaction().getReadSet());
				else
					replicationManager.initiateLeaderElection(waitingRequest.getTransaction().getTransactionID(), waitingRequest.getTransaction().getWriteSet());
			}
			else{
				this.AddLogEntry("Leader not set. No waiting Requests found. Initiaing Leader Election with dummy values\n", Level.INFO);
				replicationManager.initiateLeaderElection("-1", null);
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
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
	}

	/**
	 * Method to send Leader message to MDS
	 * @param dest
	 * @param message
	 */
	private void sendMsgToMDS(NodeProto dest, LeaderMsg message)
	{
		this.AddLogEntry("Sent :: "+message+"to "+dest.getHost()+":"+dest.getPort()+"\n", Level.INFO);
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
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
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
	}


	/**
	 * Method to send Paxos Msg to the designated replica/Leader
	 * @param dest
	 * @param msg
	 */
	private void sendPaxosMsg(NodeProto dest, PaxosMsg msg){
		//System.out.println("Sent " + msg+" from "+nodeAddress.getHost()+":"+nodeAddress.getPort() +" to "+dest.getHost()+":"+dest.getPort()+"\n");
		//this.AddLogEntry("Sent "+msg+"\n");
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();	
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
			for(ElementProto element : trans.getReadSet().getElementsList())
			{
				if(!lockTable.getReadLock(element.getRow(), trans.getTransactionID()))
					isReadLock = false;
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
			else
				read_response = new ClientOpMsg(nodeAddress, trans, ClientOPMsgType.READ_RESPONSE, false);

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
				
				uidTransMap.put( "P"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.PREPARE));
				initReplication("P"+trans.getTransactionID(), trans.getWriteSet());
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

	public synchronized void initReplication(String uid, ElementsSetProto acceptedValue)
	{
		int newLogPosition = logCounter.incrementAndGet();
		AddLogEntry("Initiating new Replication Instance for Log Position "+newLogPosition);
		uidTologPositionMap.put(uid, newLogPosition);
		logPostionToUIDMap.put(newLogPosition, uid);
		replicationManager.initiateReplicationInstance(uid, newLogPosition, acceptedValue);
	}
	
	public synchronized void initReplicationForAbort(String uid, ElementsSetProto acceptedValue, NodeProto source)
	{
		int newLogPosition = logCounter.incrementAndGet();
		AddLogEntry("New Abort Transaction "+uid +" for log position "+newLogPosition);
		uidTologPositionMap.put(uid, newLogPosition);
		logPostionToUIDMap.put(newLogPosition, uid);
		writeToPaxLogFile(newLogPosition, "ABORTED", acceptedValue);
		replicationManager.initiateReplicationInstance(uid, newLogPosition, acceptedValue);
	}

	
	public synchronized void replicationResponseNewEntry(String uid, ElementsSetProto acceptedvalue, NodeProto source, int logPos )
	{
		uidTransMap.remove(uid);
		AddLogEntry("Not able to acquire locks. Hence Aborting the transaction");
		writeToPaxLogFile(logPos, "ABORTED", acceptedvalue);
		TransactionSource tempTransSource = uidTransMap.get(uid);
		lockTable.releaseLocksOfAborted(tempTransSource.trans);
		AddLogEntry("Sending client response(ABORT) to "+tempTransSource.getSource()+" for "+uid);
		TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
		SendTwoPCMessage(message, source);
	}
	
	
	/**
	 * Method to iniate Decide phase for a Paxos Instance
	 * @param msg
	 */
	public synchronized void initiateDecide(String uid, ElementsSetProto acceptedvalue , NodeProto source)
	{
		if(uid.startsWith("C")){
			Boolean isWritten = localResource.WriteResource(acceptedvalue);

			if(isWritten){
				writeToPaxLogFile(uidTologPositionMap.get(uid), "COMMITED", acceptedvalue);
				AddLogEntry("COMMITED. Releasing the resources for "+uid);
				releaseLocks(acceptedvalue, uid.substring(1));
				if(uidTransMap.containsKey(uid.substring(1))){
					ElementsSetProto readSet = uidTransMap.get(uid.substring(1)).trans.getReadSet();
					if(readSet != null)
						lockTable.releaseReadLocks(uidTransMap.get(uid.substring(1)).trans.getReadSet(), uid.substring(1), true);
					//FIX ME: send DECIDE msg to participants
				}
				replicationManager.sendDecideMsg(uid, uidTologPositionMap.get(uid), "");
				
				if(isLeader)
				{
					AddLogEntry("Decided on a value(Commited). Sending response to TPC "+uid);
					sendReplicationInstanceResponse(uid);
				}
			}
			else{
				//send client response
				///FIX me . send to TPC
				writeToPaxLogFile(uidTologPositionMap.get(uid), "ABORTED", acceptedvalue);
				if(isLeader){
					TransactionSource tempTransSource = uidTransMap.get(uid);					
					lockTable.releaseLocksOfAborted(tempTransSource.trans);
					TwoPCMsg message = new TwoPCMsg(nodeAddress, tempTransSource.getTrans(), TwoPCMsgType.ABORT, true);
					AddLogEntry("Sending client response(ABORT)"+message+" to "+tempTransSource.getSource() );
					SendTwoPCMessage(message, source);
				}
			}
		}
		else if(uid.startsWith("P")){//Pax instance for PREPARE phase. Just obtained read locks
			AddLogEntry("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY for a PREPARE transaction "+uid+"\n");
			//FIX ME: send DECIDE msg to participants
			writeToPaxLogFile(uidTologPositionMap.get(uid), "PREPARED", acceptedvalue);
			
			replicationManager.sendDecideMsg(uid, uidTologPositionMap.get(uid), "PREPARE");
			if(isLeader){
				AddLogEntry("Done with PREPARE phase. Sending response to TPC ");
				sendReplicationInstanceResponse(uid);
			}
		}
		else if(uid.startsWith("A"))
		{
			//FIX ME: send DECIDE msg to participants
			AddLogEntry("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY for a ABORT transaction "+uid+"\n");
			lockTable.releaseLocksOfAborted(uidTransMap.get(uid).trans);
			
			replicationManager.sendDecideMsg(uid, uidTologPositionMap.get(uid), "ABORT");
			if(isLeader){
				AddLogEntry("Done with ABORT. Sending response to TPC ");
				sendReplicationInstanceResponse(uid);
			}
		}
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
	 * Method to release locks for the given readset or writeset
	 * @param elementsSetProto
	 * @param uid
	 */
	private synchronized void releaseLocks(ElementsSetProto elementsSetProto, String uid)
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
	private synchronized void sendReplicationInstanceResponse(String uid)
	{
		//send response to TPC
		TransactionSource tempTransSource = uidTransMap.get(uid);

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
	private synchronized void handleTwoPCCommitMessage(TwoPCMsg msg)
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

				uidTransMap.put( "C"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.COMMIT));
				initReplication("C"+trans.getTransactionID(), trans.getWriteSet());
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
			if(!uidTologPositionMap.containsKey("A"+uid)){
				replicationManager.checkToRemovePrepareTxn(uidTologPositionMap.get("P"+uid));
				uidTransMap.put( "A"+trans.getTransactionID(), new TransactionSource(trans, msg.getSource(), TwoPCMsgType.ABORT));
				lockTable.releaseLocksOfAborted(trans);
				initReplicationForAbort("A"+trans.getTransactionID(), trans.getWriteSet(), msg.getSource());
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
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();
	}

	
	/**
	 * Method used to append content to Transactional Log file
	 * @param counter
	 * @param type
	 * @param acceptedValue
	 */
	public void writeToPaxLogFile(int counter, String type, ElementsSetProto acceptedValue){

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
	}

}
