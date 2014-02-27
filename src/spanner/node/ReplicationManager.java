package spanner.node;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.PLeaderState;
import spanner.common.Common.PaxosMsgType;
import spanner.common.Common.TwoPCMsgType;
import spanner.message.PaxosMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;

public class ReplicationManager implements Runnable{
	
	PaxosAcceptor paxosAcceptor = null;
	BallotNumber ballotNo = null;
	private HashSet<Integer> pendingPaxosInstances = null;
	private ConcurrentHashMap<Integer, PaxosInstance> logPositionToPaxInstanceMap = null;
	private ConcurrentHashMap<String, PaxosInstance> uidPaxosInstanceMap = null;
	public PaxosInstance dummyInstance = null;
	
	public ReplicationManager(PaxosAcceptor paxosAcceptor)
	{
		this.paxosAcceptor = paxosAcceptor;
		ballotNo = new BallotNumber(0, paxosAcceptor.myId);
		pendingPaxosInstances = new HashSet<Integer>();
		logPositionToPaxInstanceMap = new ConcurrentHashMap<Integer, PaxosInstance>();
		uidPaxosInstanceMap = new ConcurrentHashMap<String, PaxosInstance>();
		dummyInstance = new PaxosInstance(null,  null);
	}
	
	
	
	/**
	 * Daemon thread executing in the background to check for aborts for pending transaction
	 */
	public void run()
	{
		while(true){
			checkForPendingTrans();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Method used to check pending trnsactions for aborts
	 */
	private synchronized void checkForPendingTrans() 
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
				if(paxInstance.decides.size() == 0)
				{
					pendingPaxosInstances.remove(logPos);
					paxosAcceptor.handleAbortsForReplicationInstance(uid, logPos, paxInstance.getAcceptedValue());
				}
			}
			else{
				//AddLogEntry("Checking txn "+logPos+" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			}
		}
	}
	
	
	public synchronized void handleIncomingMessage(PaxosMsg msg, PLeaderState state)
	{
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
			handlePaxosDecideMessage(msg);
		}
		else{
			AddLogEntry("Paxos Msg :: Not falling in any case "+msg+". Ignoring silently");
		}
	}
	
	/**
	 * Method to initiate PAXOS PREPARE phase for a new log position
	 */
	public synchronized void initiateLeaderElection()
	{
		//FIX ME
		//implement later
	}
	/*public void initiatePreparePhase()
	{
		int newLogPosition = logCounter;
		PaxosInstance paxInstance = new PaxosInstance("-1", ballotNo);
		logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
		PaxosMsg msg = new PaxosMsg(nodeAddress, "-1", PaxosMsgType.PREPARE, ballotNo);
		msg.setLogPositionNumber(newLogPosition);
		SendPrepareMessage(msg);
	}*/

	
	
	/**
	 * Method to initiate PAXOS PREPARE phase for a new log position
	 * @param uid
	 * @param acceptedValue
	 */
	public synchronized void initiateLeaderElection(String uid, ElementsSetProto acceptedValue){
		//FIX ME
		//implement later
	}
	/*public synchronized void initiatePreparePhase(String uid, ElementsSetProto acceptedValue)
	{
		int newLogPosition = logCounter;
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo, acceptedValue);
		logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
		PaxosMsg msg = new PaxosMsg(nodeAddress, uid, PaxosMsgType.PREPARE, ballotNo);
		msg.setLogPositionNumber(newLogPosition);
		SendPrepareMessage(msg);
	}*/
	
	/**
	 * Method to process PAXOS PREPARE message
	 * @param msg
	 */
	public synchronized void handlePrepareMessage(PaxosMsg msg){
		//FIX ME:
		//implement later
	}
/*	private synchronized void handlePrepareMessage(PaxosMsg msg)
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
	}*/

	/**
	 * Method to process ACK messages from replicas
	 * @param msg
	 */
	public synchronized void handleAckMessage(PaxosMsg msg){
		//FIX ME
		// implement later
	}
/*	private synchronized void handleAckMessage(PaxosMsg msg)
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
	}*/

	

	
	public synchronized void initiateReplicationInstance(String uid, int logPos, ElementsSetProto acceptedValue)
	{
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo, acceptedValue);
		paxInstance.setAcceptSent();
		paxInstance.setTimeStamp(new Date().getTime());
		paxInstance.addtoAcceptList(paxosAcceptor.nodeAddress);
		pendingPaxosInstances.add(logPos);
		uidPaxosInstanceMap.put(uid, paxInstance);
		logPositionToPaxInstanceMap.put(logPos, paxInstance);
		AddLogEntry("Sending accept msgs to all acceptors \n");
		PaxosMsg message = new PaxosMsg(paxosAcceptor.nodeAddress, uid,PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), acceptedValue);
		message.setLogPositionNumber(logPos);
		sendAcceptMsg(message);
	}
	
	/*public synchronized void initiateReplicationInstanceforAbort(String uid, int logPos, ElementsSetProto acceptedValue)
	{
		PaxosInstance paxInstance = new PaxosInstance(uid, ballotNo, acceptedValue);
		paxInstance.setAcceptSent();
		paxInstance.setTimeStamp(new Date().getTime());
		pendingPaxosInstances.add(logPos);
		paxInstance.addtoAcceptList(paxosAcceptor.nodeAddress);

		uidPaxosInstanceMap.put(uid, paxInstance);
		logPositionToPaxInstanceMap.put(logPos, paxInstance);
		PaxosMsg message = new PaxosMsg(paxosAcceptor.nodeAddress, uid,PaxosMsgType.ACCEPT, paxInstance.getBallotNumber(), acceptedValue);
		message.setLogPositionNumber(logPos);
		AddLogEntry("Sending accept msgs to all acceptors ");
	}*/
	
	
	public synchronized void checkToRemovePrepareTxn(int logPos)
	{
		if(pendingPaxosInstances.contains(logPos))
		{
			AddLogEntry("Aborting paxInstance of PREPARE txn for the received Abort trans ");
			pendingPaxosInstances.remove(logPos);
		}	
	}
	/**
	 * Mehtod to process incoming ACCEPT msg
	 * @param msg
	 */
	public synchronized void handlePaxosAcceptMessage(PaxosMsg msg)
	{
		String uid = msg.getUID();
		AddLogEntry("\nHandling ACCEPT msg "+msg);

		if(paxosAcceptor.logPostionToUIDMap.containsKey(msg.getLogPositionNumber()))
		{	
			AddLogEntry("Already having paxInstance for the same log Position ", Level.FINE);
			PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
			if((msg.getBallotNumber().compareTo(paxInstance.getBallotNumber())) >=0 ){
				paxInstance.setBallotNumber(msg.getBallotNumber());
				int prevCount = paxInstance.accepts.size();
				paxInstance.accepts.add(msg.getSource());
				int newCount = paxInstance.accepts.size();
				if(newCount > prevCount){
					if(newCount >  paxosAcceptor.acceptorsCount/2)
					{
						if(!paxInstance.isDecideSent)
						{
							AddLogEntry("Reached majority of acceptors, but DECIDE not yet sent. DECIDING and sending DECIDE msg to all replicas");						
							paxInstance.isDecideSent = true;
							paxInstance.addToDecideList(paxosAcceptor.nodeAddress);
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
							paxosAcceptor.initiateDecide(msg.getUID(), msg.getAcceptValue(), msg.getSource());
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
			paxInstance.addtoAcceptList(paxosAcceptor.nodeAddress);
			paxInstance.setTimeStamp(new Date().getTime());

			boolean isWriteLock = true;
			for(ElementProto element : msg.getAcceptValue().getElementsList())
			{
				if(!paxosAcceptor.lockTable.getWriteLock(element.getRow(), uid.substring(1)))				
					isWriteLock = false;
			}

			if(isWriteLock){
				paxInstance.setAcceptSent();
				uidPaxosInstanceMap.put(uid, paxInstance );
				//paxosAcceptor.initReplication(uid, msg.getAcceptValue(), msg.getLogPositionNumber());
				int newLogPosition = msg.getLogPositionNumber();
				AddLogEntry("Initiating paxos instance for log position "+newLogPosition);
				paxosAcceptor.uidTologPositionMap.put(uid, newLogPosition);
				paxosAcceptor.logPostionToUIDMap.put(newLogPosition, uid);
				logPositionToPaxInstanceMap.put(newLogPosition, paxInstance);
				
				if(!pendingPaxosInstances.contains(newLogPosition)){
					pendingPaxosInstances.add(newLogPosition);
				}
				AddLogEntry("Sending Accept Msgs to all acceptors");
				PaxosMsg message = new PaxosMsg(paxosAcceptor.nodeAddress, uid,PaxosMsgType.ACCEPT, msg.getBallotNumber(),msg.getAcceptValue());
				message.setLogPositionNumber(msg.getLogPositionNumber());
				sendAcceptMsg( message );
			}
			else{
				uidPaxosInstanceMap.put(uid, paxInstance );
				dummyInstance = new PaxosInstance("A"+uid.substring(1), null);
				dummyInstance.setAcceptedValue(msg.getAcceptValue());
				uidPaxosInstanceMap.put("A"+uid.substring(1), dummyInstance);
				int newLogPosition = paxosAcceptor.logCounter.incrementAndGet();
				logPositionToPaxInstanceMap.put(newLogPosition, dummyInstance);
				paxosAcceptor.replicationResponseNewEntry(uid, msg.getAcceptValue(), msg.getSource(), newLogPosition);
			}
		}
	}
	
	
	public void sendDecideMsg(String uid, int logPos, String msgType)
	{
		PaxosInstance paxInstance = uidPaxosInstanceMap.get(uid);
		AddLogEntry("Sending DECIDE to ALL ACCEPTORS after REACHING MAJORITY for a "+msgType+" transaction "+uid+"\n");
		PaxosMsg message = new PaxosMsg(paxosAcceptor.nodeAddress, uid, PaxosMsgType.DECIDE, paxInstance.getBallotNumber(), paxInstance.getAcceptedValue());
		message.setLogPositionNumber(logPos);
		sendDecideMsg(message);
	}


	/**
	 * Method to process Decide Message
	 * @param msg
	 */
	public synchronized void handlePaxosDecideMessage(PaxosMsg msg)
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
					paxInstance.addToDecideList(paxosAcceptor.nodeAddress);
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
					paxosAcceptor.initiateDecide(msg.getUID(), msg.getAcceptValue(), msg.getSource());
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
			paxInstance.addToDecideList(paxosAcceptor.nodeAddress);
			paxInstance.setTimeStamp(System.currentTimeMillis());
			paxInstance.isDecideSent = true;
			paxInstance.isCommited = true;
			uidPaxosInstanceMap.put(uid, paxInstance );
			logPositionToPaxInstanceMap.put(msg.getLogPositionNumber(), paxInstance);
			if(pendingPaxosInstances.contains(msg.getLogPositionNumber()))
				pendingPaxosInstances.remove(msg.getLogPositionNumber());
			paxosAcceptor.initiateDecide(msg.getUID(), msg.getAcceptValue(), msg.getSource());
		}
	}

	/**
	 * Method to send Decide message to all replicas 
	 * @param msg
	 */
	private void sendDecideMsg(PaxosMsg msg)
	{
		for(NodeProto node: paxosAcceptor.acceptors)
		{
			if(!node.equals(paxosAcceptor.nodeAddress))
			{
				AddLogEntry("Sending DECIDE msg "+msg+" to "+node.getHost()+":"+node.getPort()+"\n");
				sendPaxosMsg(node, msg);
			}
		}
	}
	
	/**
	 * Method to send Paxos Msg to the designated replica/Leader
	 * @param dest
	 * @param msg
	 */
	private void sendPaxosMsg(NodeProto dest, PaxosMsg msg){
		//System.out.println("Sent " + msg+" from "+nodeAddress.getHost()+":"+nodeAddress.getPort() +" to "+dest.getHost()+":"+dest.getPort()+"\n");
		//this.AddLogEntry("Sent "+msg+"\n");
		ZMQ.Socket pushSocket = paxosAcceptor.context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0 );
		pushSocket.close();	
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
		for(NodeProto node: paxosAcceptor.acceptors)
		{
			if(!paxosAcceptor.nodeAddress.equals(node))
				sendPaxosMsg(node, msg);
		}
	}

	/**
	 * Method Accept messages to all replicas
	 * @param msg
	 */
	public void sendAcceptMsg(PaxosMsg msg)
	{
		for(NodeProto node: paxosAcceptor.acceptors)
		{
			if(!node.equals(paxosAcceptor.nodeAddress))
			{
				AddLogEntry("Sending ACCEPT message "+msg+"to "+node.getHost()+":"+node.getPort()+"\n");
				sendPaxosMsg(node, msg);
			}
		}
	}

	
	/**
	 * Method to append content to Log with custom Log Level
	 * @param message
	 * @param level
	 */
	public void AddLogEntry(String message, Level level){	
		paxosAcceptor.AddLogEntry(message, level);	
	}

	/**
	 * Method  to append content to Log with defualt Log Level(INFO)
	 * @param message
	 */
	public void AddLogEntry(String message){	
		paxosAcceptor.AddLogEntry(message, Level.INFO);		
	}
	
	
	
}
