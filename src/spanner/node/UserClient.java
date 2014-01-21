package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.MetaDataMsgType;
import spanner.message.ClientOpMsg;
import spanner.message.MetaDataMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.NodeProto;

public class UserClient extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;
	int port = -1;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	NodeProto transClient ;
	NodeProto clientNode;
	public UserClient(String clientID, int port) throws IOException
	{
		super(clientID);
		context = ZMQ.context(1);
		InetAddress addr = InetAddress.getLocalHost();
		clientNode = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		System.out.println(" Listening to "+Common.getLocalAddress(port));
		socket.bind(Common.getLocalAddress(port));
		this.port = port;
		String[] transcli = Common.getProperty("transClient").split(":");
		if(transcli[0].equalsIgnoreCase("localhost"))
			transClient = NodeProto.newBuilder().setHost("127.0.0.1").setPort(Integer.parseInt(transcli[1])).build();
		else
			transClient = NodeProto.newBuilder().setHost(transcli[0]).setPort(Integer.parseInt(transcli[1])).build();

	}

	public void sendMessages() throws NumberFormatException, IOException
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("1.Initiate Trans 2. Exit");
		int option;

		option = Integer.parseInt(br.readLine());

		while(option != 2)
		{
			System.out.println("Enter details for new trans ");
			HashMap<String, ArrayList<String>> readSet = new HashMap<String, ArrayList<String>>();
			System.out.println("Enter the no of rows for reads");
			int rowCount = Integer.parseInt(br.readLine());
			int count = 0;

			while(count < rowCount)
			{
				System.out.println("Enter record in the following format: rowKey:col1,col2,col2....");
				String line = br.readLine();
				String inputs[] = line.split(":");
				String[] cols = inputs[1].split(",");
				ArrayList<String> colList = new ArrayList<String>();
				for(String col: cols)
					colList.add(col);
				readSet.put(inputs[0], colList);
				count++;
			}

			System.out.println("Enter the no of rows for writes ");
			rowCount = Integer.parseInt(br.readLine());
			count = 0;
			HashMap<String, HashMap<String, String>> writeSet = new HashMap<String, HashMap<String, String>>();
			while(count < rowCount)
			{
				System.out.println("Enter record in the following format: rowKey:col1,colVal1;col2,colVal2;col3,colVal3;....");
				String line = br.readLine();
				String inputs[] = line.split(":");
				String[] cols = inputs[1].split(";");
				HashMap<String, String> colList = new HashMap<String, String>();
				for(String col: cols)
				{
					String[] colEntry = col.split(",");
					colList.put(colEntry[0], colEntry[1]);
				}
				writeSet.put(inputs[0], colList);
				count++;
			}

			initiateTrans(readSet, writeSet);

		}

		System.out.println("Enter option 1.Continue 2.Exit ");
		option = Integer.parseInt(br.readLine());
	}


	private void initiateTrans(HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
	{
		MetaDataMsg msg = new MetaDataMsg(clientNode, readSet, writeSet, MetaDataMsgType.REQEUST);
		sendMetaDataMsg(msg);
	}

	private void sendMetaDataMsg(MetaDataMsg msg)
	{
		System.out.println("Sending Client Request "+msg);
		socketPush = context.socket(ZMQ.PUSH);
		//System.out.println(" "+metadataService.getHost()+":"+metadataService.getPort());
		socketPush.connect("tcp://"+transClient.getHost()+":"+transClient.getPort());
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
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
					if (msgwrap.getmessageclass() == ClientOpMsg.class)
					{
						ClientOpMsg message = (ClientOpMsg)msgwrap.getDeSerializedInnerMessage();

						ProcessClientResponse(message);

					}
					else {

					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		this.close();
		socket.close();
		context.term();
	}


	private void ProcessClientResponse(ClientOpMsg msg)
	{
		System.out.println("Received Response from "+msg.getSource());
		System.out.println(" "+msg.getTransaction());
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 2)
		{
			System.out.println("ClientID port ");
		}

		int port = Integer.parseInt(args[1]);
		UserClient client = new UserClient(args[0], port);
		new Thread(client).start();
		client.sendMessages();
	}

}
