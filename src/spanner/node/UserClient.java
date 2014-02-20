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
	public UserClient(String clientID, int port, boolean isNew) throws IOException
	{
		super(clientID, isNew);
		context = ZMQ.context(1);
		InetAddress addr = InetAddress.getLocalHost();
		clientNode = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		AddLogEntry("Listening to messages at "+Common.getLocalAddress(port));
		socket.bind("tcp://*:"+port);
		this.port = port;
		String[] transcli = Common.getProperty("transClient").split(":");
		if(transcli[0].equalsIgnoreCase("localhost"))
			transClient = NodeProto.newBuilder().setHost("127.0.0.1").setPort(Integer.parseInt(transcli[1])).build();
		else
			transClient = NodeProto.newBuilder().setHost(transcli[0]).setPort(Integer.parseInt(transcli[1])).build();

	}


	public void run()
	{
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
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
						throw new IllegalStateException("Message not expected of type"+msgwrap.getmessageclass()+". Ignoring silently");
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		socket.close();
		context.term();
	}


	/**
	 * Method used to get input data from user
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public void init() throws NumberFormatException, IOException
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
			System.out.println("Enter option 1.New Trans 2.Exit ");
			option = Integer.parseInt(br.readLine());
		}
	}

	/**
	 * Method to initiate transaction
	 * @param readSet
	 * @param writeSet
	 */
	private void initiateTrans(HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
	{
		MetaDataMsg msg = new MetaDataMsg(clientNode, readSet, writeSet, MetaDataMsgType.REQEUST);
		sendMetaDataMsg(msg);
	}

	/**
	 * Method to send msg to MDS
	 * @param msg
	 */
	private void sendMetaDataMsg(MetaDataMsg msg)
	{
		AddLogEntry("Sending Client Request "+msg+"to "+transClient.getHost()+":"+transClient.getPort()+"\n");
		socketPush = context.socket(ZMQ.PUSH);
		socketPush.connect("tcp://"+transClient.getHost()+":"+transClient.getPort());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
	}

	/**
	 * Method to process transaction response
	 * @param msg
	 */
	private void ProcessClientResponse(ClientOpMsg msg)
	{
		AddLogEntry("Received Client Response "+msg);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 3)
			System.out.println("Usage: UserCient <ClientID> <port> <isNewLog>");

		int port = Integer.parseInt(args[1]);
		boolean isNew = Boolean.parseBoolean(args[2]);
		UserClient client = new UserClient(args[0], port, isNew);
		new Thread(client).start();
		client.init();
	}

}
