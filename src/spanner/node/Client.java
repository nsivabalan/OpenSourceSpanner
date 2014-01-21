package spanner.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.MessageWrapper;
import spanner.common.Resource;
import spanner.common.Common.State;
import spanner.message.BcastMsg;
import spanner.message.ClientOpMsg;
import spanner.node.Acceptor.TransactionStatus;

public class Client extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;
	int port = -1;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	HashMap<String , String> shardLocator = null;

	public Client(String clientID, int port, ArrayList<String> shardIds) throws IOException
	{
		super(clientID);
		context = ZMQ.context(1);

		shardLocator = new HashMap<String, String>();
		for(String shard : shardIds){
			String shardDetails = Common.getProperty(shard);
			String[] shards = shardDetails.split(";");
			String subPort = Common.getProperty(shard+"Leader");
			System.out.println(" shard "+shard+" "+Common.getProperty(shards[0]));
			shardLocator.put(shard, "tcp://"+Common.getProperty(shards[0]));
		}

		socket = context.socket(ZMQ.PULL);
		System.out.println(" Listening to "+Common.getLocalAddress(port));
		socket.bind(Common.getLocalAddress(port));
		this.port = port;


	}

	public void sendMessages()
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("1.Choose Paxos Leader 2. Exit");
		int option;
		try {
			option = Integer.parseInt(br.readLine());

			while(option != 2)
			{
				System.out.println("Enter Paxos Leader shard Id to contact :: S1,S2 ");
				String shardId = br.readLine();
				System.out.println("1.Read value, 2. Write Value, 3.Exit ");
				int option2 = Integer.parseInt(br.readLine());
				while(option2 < 3)
				{
					if(option2 == 1)
					{
						System.out.println("Enter the no of rows");
						int rowCount = Integer.parseInt(br.readLine());
						int count = 0;
						HashMap<String, ArrayList<String>> readSet = new HashMap<String, ArrayList<String>>();
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
						UUID uid = java.util.UUID.randomUUID();	
						ClientOpMsg message = new ClientOpMsg(Common.getLocalAddress(port), ClientOPMsgType.READ, readSet, null, uid );

						SendClientOpMessage(message, shardId);
					}
					else if(option2 == 2)
					{
						System.out.println("Enter the no of rows");
						int rowCount = Integer.parseInt(br.readLine());
						int count = 0;
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
							//	System.out.println("Col entry");
								//for(String str: colEntry)
								//System.out.print(" "+str);
								colList.put(colEntry[0], colEntry[1]);
							}
							writeSet.put(inputs[0], colList);
							count++;
						}

						UUID uid = java.util.UUID.randomUUID();	
						ClientOpMsg message = new ClientOpMsg(Common.getLocalAddress(port), ClientOPMsgType.WRITE, null, writeSet, uid);
						SendClientOpMessage(message, shardId);
					}
					else break;
					System.out.println("Enter option 1.Read value, 2. Write Value, 3.Exit ");
					option2 = Integer.parseInt(br.readLine());
				}
			}

		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void SendClientOpMessage(ClientOpMsg message, String shard)
	{
		System.out.println("Sending Client Request "+message);
		socketPush = context.socket(ZMQ.PUSH);
		System.out.println(" "+shardLocator.get(shard));
		socketPush.connect(shardLocator.get(shard));
		System.out.println(" "+socketPush.getLinger());
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(message), message.getClass());
		socketPush.send(msgwrap.getSerializedMessage().getBytes(), 0);
		socketPush.close();
		
	}

	public void run()
	{
		System.out.println("Waiting for messages "+socket.toString());
		while (!Thread.currentThread ().isInterrupted ()) {
			String receivedMsg = new String( socket.recv(0)).trim();
			System.out.println("Received Messsage "+receivedMsg);
			MessageWrapper msgwrap = MessageWrapper.getDeSerializedMessage(receivedMsg);
			if (msgwrap != null)
			{
				try {
					if (msgwrap.getmessageclass() == ClientOpMsg.class)
					{
						ClientOpMsg message = (ClientOpMsg)msgwrap.getDeSerializedInnerMessage();
						ProcessClientOpMessage(message);
					}
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		this.close();
		socket.close();
		context.term();
	}

	private void ProcessClientOpMessage(ClientOpMsg message)
	{
		System.out.println("Received Client Response "+message.getUid());
		System.out.println(""+message.toString());
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 3)
		{
			System.out.println("ClientID port <shardIds(comma separated)>");
		}

		int port = Integer.parseInt(args[1]);
		String[] shardIds = args[2].split(",");
		ArrayList<String> shards = new ArrayList<String>(Arrays.asList(shardIds));
		Client client = new Client(args[0], port, shards);
		new Thread(client).start();
		client.sendMessages();
	}

}
