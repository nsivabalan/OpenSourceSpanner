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

public class PushClient implements Runnable{

	ZMQ.Context context = null;

	ZMQ.Socket socket = null;
	NodeProto myNode = null;
	NodeProto leaderProto = null;
	Long startTime = null;
	public PushClient(String host, int port) throws IOException
	{
		context = ZMQ.context(1);

		System.out.println("Connecting to Server");

		socket = context.socket(ZMQ.PULL);
		socket.bind("tcp://*:5992");
		InetAddress addr = InetAddress.getLocalHost();
		myNode = NodeProto.newBuilder().setHost(addr.getLocalHost().getHostAddress()).setPort(5991).build();
		leaderProto = NodeProto.newBuilder().setHost(host).setPort(port).build();
		

	}

	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);

			try {
				LeaderMsg message  = (LeaderMsg)msgwrap.getDeSerializedInnerMessage();
				ReceiveResponse(message);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		socket.close();
		context.term();
	}


	private void ReceiveResponse(LeaderMsg msg)
	{
		System.out.println(" "+msg);
		System.out.println("Recieved response in "+(System.currentTimeMillis() - startTime));
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
		System.out.println("Sending "+msg+" to "+leaderProto);
		startTime = System.currentTimeMillis();
		LeaderMsg message = new LeaderMsg(myNode, LeaderMsgType.REQUEST, "testrequest ");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		pushSocket.send(msgwrap.getSerializedMessage().getBytes(), 0);
		pushSocket.close();
	}

	
	public static void main (String[] args) throws IOException{

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		PushClient obj = new PushClient(host, port);
		new Thread(obj).start();
		obj.sendRequest();
	}
}