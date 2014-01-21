package spanner.metadataservice;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.MetaDataMsgType;
import spanner.common.Common.PaxosDetailsMsgType;
import spanner.common.Common.PaxosMsgType;
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
	
	public MDService(String MDservice) throws IOException
	{
		super(MDservice);
		context = ZMQ.context(1);

		String[] mds = Common.getProperty("mds").split(":");
		this.port = Integer.parseInt(mds[1]);
		InetAddress addr = InetAddress.getLocalHost();
		mdsNode = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		System.out.println(" Listening to "+Common.getLocalAddress(port));
		socket.bind(Common.getLocalAddress(port));
		metadataService = new MetaDS();

	}
	
	
	public void run()
	{
		System.out.println("Waiting for messages "+socket.toString());
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
				}
				 catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		
		socket.close();
		context.term();
	}
	
	private void ProcessRequestAcceptors(PaxosDetailsMsg msg)
	{
		ArrayList<NodeProto> acceptors = metadataService.getAcceptors(msg.getShardId());
		PaxosDetailsMsg message = new PaxosDetailsMsg(mdsNode, msg.getShardId(), PaxosDetailsMsgType.ACCEPTORS);
		message.setReplicas(acceptors);
		SendPaxosDetailsMsg(msg.getSource(), message);
	}
	
	private void ProcessRequestLeader(PaxosDetailsMsg msg)
	{
		NodeProto shardLeader = metadataService.getShardLeader(msg.getShardId());
		PaxosDetailsMsg message = new PaxosDetailsMsg(mdsNode, msg.getShardId(), PaxosDetailsMsgType.LEADER);
		message.setShardLeader(shardLeader);
		SendPaxosDetailsMsg(msg.getSource(), message);
	}
	
	private void ProcessMetaDataMessage(MetaDataMsg msg)
	{
		TransactionMetaDataProto trans = metadataService.getTransactionDetails(msg.getSource(), msg.getReadSet(), msg.getWriteSet(), msg.getUID());
		MetaDataMsg message = new MetaDataMsg(mdsNode, trans, MetaDataMsgType.RESPONSE);
		
		SendMetaData(msg.getSource(), message);
	}
	
	
	private void SendPaxosDetailsMsg(NodeProto dest, PaxosDetailsMsg msg)
	{
		System.out.println("Sending Client Request "+msg);
		socketPush = context.socket(ZMQ.PUSH);
	//	System.out.println(" "+dest.getHost()+":"+dest.getPort());
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}
	
	private void SendMetaData(NodeProto dest, MetaDataMsg msg)
	{
		System.out.println("Sending Client Request "+msg);
		socketPush = context.socket(ZMQ.PUSH);
		System.out.println(" "+dest.getHost()+":"+dest.getPort());
		socketPush.connect("tcp://"+dest.getHost()+":"+dest.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	public static void main(String args[])
	{
		try {
			MDService obj = new MDService("MDS Service");
			new Thread(obj).start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
