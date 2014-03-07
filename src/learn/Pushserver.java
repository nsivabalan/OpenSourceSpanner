package learn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.LeaderMsgType;
import spanner.message.LeaderMsg;
import spanner.message.MetaDataMsg;
import spanner.protos.Protos.NodeProto;

public class Pushserver implements Runnable{

	ZMQ.Context context = null;

	ZMQ.Socket socket = null;
	NodeProto myNode = null;
	NodeProto leaderProto = null;
	public Pushserver() throws IOException
	{
		context = ZMQ.context(1);

		System.out.println("Connecting to Server");

		socket = context.socket(ZMQ.PULL);
		socket.bind("tcp://*:5991");
		InetAddress addr = InetAddress.getLocalHost();
		myNode = NodeProto.newBuilder().setHost(addr.getLocalHost().getHostAddress()).setPort(5991).build();
		

	}

	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);

			try {
				LeaderMsg message  = (LeaderMsg)msgwrap.getDeSerializedInnerMessage();
				System.out.println("Received msg "+message);
				sendResponse(message);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		socket.close();
		context.term();
	}


	private void sendResponse(LeaderMsg msg)
	{
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+msg.getSource().getHost()+":"+msg.getSource().getPort());

		LeaderMsg message = new LeaderMsg(myNode, LeaderMsgType.RESPONSE, "test");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0);
		pushSocket.close();
	}


	public String getAddress(String host, int port)
	{
		return new String("tcp://"+host+":"+port);
	}

	public void sendRequest()
	{
		String msg = "test msg";
		ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
		pushSocket.connect("tcp://"+leaderProto.getHost()+":"+leaderProto.getPort());

		LeaderMsg message = new LeaderMsg(myNode, LeaderMsgType.REQUEST, "testrequest ");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0);
		pushSocket.close();
	}

	
	public static void main (String[] args) throws IOException{

		Pushserver obj = new Pushserver();
		new Thread(obj).start();

	}
}