package spanner.metadataservice;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.logging.Level;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.LeaderMsgType;
import spanner.common.Common.MetaDataMsgType;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosMsgType;
import spanner.message.LeaderMsg;
import spanner.message.MetaDataMsg;
import spanner.message.PaxosDetailsMsg;
import spanner.node.Node;
import spanner.protos.Protos.NodeProto;
import spanner.protos.Protos.TransactionMetaDataProto;

public class MDService extends Node implements Runnable{
	
	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;
	int port ;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	MetaDS metadataService ;
	NodeProto mdsNode;
	
	public MDService(String MDservice, boolean isNew) throws IOException
	{
		super(MDservice, isNew, false);
		context = ZMQ.context(1);
		String[] mds = Common.getProperty("mds").split(":");
		this.port = Integer.parseInt(mds[1]);
		InetAddress addr = InetAddress.getLocalHost();
		mdsNode = NodeProto.newBuilder().setHost(mds[0]).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		socket.bind("tcp://*:"+port);
		AddLogEntry("Listening to messages @ "+mdsNode.getHost()+":"+port+"\n");
		metadataService = new MetaDS(isNew);
	}
	
	
	public void run()
	{
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
						if(message.getMsgType() == MetaDataMsgType.REQEUST)
							ProcessMetaDataMessage(message);
						
					}
				else if (msgwrap.getmessageclass() == PaxosDetailsMsg.class)
				{
					PaxosDetailsMsg message = (PaxosDetailsMsg)msgwrap.getDeSerializedInnerMessage();
					if(message.getMsgType() == PaxosDetailsMsgType.ACCEPTORS)
						ProcessRequestAcceptors(message);
					else if(message.getMsgType() == PaxosDetailsMsgType.LEADER)
						ProcessRequestLeader(message);
				}
				else if(msgwrap.getmessageclass() == LeaderMsg.class )
				{
					LeaderMsg message = (LeaderMsg)msgwrap.getDeSerializedInnerMessage();
					if(message.getType() == LeaderMsgType.RESPONSE)
					{
						updateLeaderInfo(message);
					}
					else{
						this.AddLogEntry("Leader Request unexpected ", Level.INFO);
					}
				}
				}
				 catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		
		socket.close();
		context.term();
	}
	
	/**
	 * Method to update Leader info for any shard
	 * @param msg
	 */
	private void updateLeaderInfo(LeaderMsg msg)
	{
		this.AddLogEntry("Leader Info received for "+msg.getShard()+", chosen leader "+msg.getSource().getHost()+":"+msg.getSource().getPort(), Level.INFO);
		if(msg.isLeader())
		{
			metadataService.setLeaderAddress(msg.getShard(), msg.getSource());
		}
	}
	
	/**
	 * Method to process requesting acceptors
	 * @param msg
	 */
	private void ProcessRequestAcceptors(PaxosDetailsMsg msg)
	{
		AddLogEntry("Received Metadata Request (Acceptors) from "+msg.getSource().getHost()+":"+msg.getSource().getPort()+" for shard "+msg.getShardId()+"\n");
		ArrayList<NodeProto> acceptors = metadataService.getAcceptors(msg.getShardId());
		PaxosDetailsMsg message = new PaxosDetailsMsg(mdsNode, msg.getShardId(), PaxosDetailsMsgType.ACCEPTORS);
		message.setReplicas(acceptors);
		SendPaxosDetailsMsg(msg.getSource(), message);
	}
	
	/**
	 * Method to process requesting Leader
	 * @param msg
	 */
	private void ProcessRequestLeader(PaxosDetailsMsg msg)
	{
		AddLogEntry("Received Metadata Request (Leader) from "+msg.getSource().getHost()+":"+msg.getSource().getPort()+" for shard "+msg.getShardId()+"\n");
		NodeProto shardLeader = metadataService.getShardLeader(msg.getShardId());
		PaxosDetailsMsg message = new PaxosDetailsMsg(mdsNode, msg.getShardId(), PaxosDetailsMsgType.LEADER);
		message.setShardLeader(shardLeader);
		SendPaxosDetailsMsg(msg.getSource(), message);
	}
	
	/**
	 * Method to process msg requesting metadata information
	 * @param msg
	 */
	private void ProcessMetaDataMessage(MetaDataMsg msg)
	{
		AddLogEntry("Received MetaData request : "+msg);
		TransactionMetaDataProto trans = metadataService.getTransactionDetails(msg.getSource(), msg.getReadSet(), msg.getWriteSet(), msg.getUID());
		MetaDataMsg message = new MetaDataMsg(mdsNode, trans, MetaDataMsgType.RESPONSE);
		
		SendMetaData(msg.getSource(), message);
	}
	
	/**
	 * Method to send Paxos Details msg to replica
	 * @param dest
	 * @param msg
	 */
	private void SendPaxosDetailsMsg(NodeProto dest, PaxosDetailsMsg msg)
	{
		AddLogEntry("Sending Paxos Details Msg "+msg+" to "+dest.getHost()+":"+dest.getPort()+"\n");
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		//System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}
	
	/**
	 * Method to send Metadata msg to trans client
	 * @param dest
	 * @param msg
	 */
	private void SendMetaData(NodeProto dest, MetaDataMsg msg)
	{
		AddLogEntry("Sending msg "+msg+" to "+dest.getHost()+":"+dest.getPort()+"\n");
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	public static void main(String args[])
	{
		try {
			if(args.length < 1)
				throw new IllegalArgumentException("Usage: MDService T/F(clear log file or no)");
			boolean isNew = Boolean.parseBoolean(args[0]);
			MDService obj = new MDService("MDS_Service", isNew);
			new Thread(obj).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
