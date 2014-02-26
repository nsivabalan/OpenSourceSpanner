package spanner.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.hbase.client.RowLock;
import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.common.Common.ClientOPMsgType;
import spanner.common.Common.MetaDataMsgType;
import spanner.message.ClientOpMsg;
import spanner.message.MetaDataMsg;
import spanner.message.TwoPCMsg;
import spanner.protos.Protos.NodeProto;

public class UserYCSBClient extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;
	int port = -1;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	NodeProto transClient ;
	NodeProto clientNode;
	File inputFile;
	HashMap<String, TransDetail> commits = null;
	HashMap<String, TransDetail> aborts = null;
	HashMap<String, TransDetail> inputs = null;
	Long beginTimeStamp = null;
	public UserYCSBClient(String clientID, int port, boolean isNew, File inputFile) throws IOException
	{
		super(clientID, isNew);
		context = ZMQ.context(1);
		InetAddress addr = InetAddress.getLocalHost();
		clientNode = NodeProto.newBuilder().setHost(addr.getHostAddress()).setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		AddLogEntry("Listening to messages at "+Common.getLocalAddress(port));
		socket.bind("tcp://*:"+port);
		this.port = port;
		this.inputFile = inputFile;
		String[] transcli = Common.getProperty("transClient").split(":");
		if(transcli[0].equalsIgnoreCase("localhost"))
			transClient = NodeProto.newBuilder().setHost("127.0.0.1").setPort(Integer.parseInt(transcli[1])).build();
		else
			transClient = NodeProto.newBuilder().setHost(transcli[0]).setPort(Integer.parseInt(transcli[1])).build();
		beginTimeStamp = System.currentTimeMillis();
		commits = new HashMap<String, TransDetail>();
		aborts = new HashMap<String, TransDetail>();
		inputs = new HashMap<String,TransDetail>();
		
	}
	
	class TransDetail{
		Long startTime;
		Long endTime;
		Long latency;
		
		public TransDetail(Long startTime)
		{
			this.startTime = startTime;
		}
		
		public Long getStartTime() {
			return startTime;
		}
		
		public Long getEndTime() {
			return endTime;
		}
		
		public long getLatency()
		{
			return this.latency;
		}
		public void setEndTime(Long endTime) {
			this.endTime = endTime;
			latency = this.endTime - this.startTime;
		}
		
		
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
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String readLen = br.readLine();
		while(readLen != null)
		{
			
			int rowCount = Integer.parseInt(readLen);
			int count = 0;
			HashMap<String, ArrayList<String>> readSet = new HashMap<String, ArrayList<String>>();
			while(count < rowCount)
			{
				String line = br.readLine();
				String inputs[] = line.split(":");
				String[] cols = inputs[1].split(",");
				ArrayList<String> colList = new ArrayList<String>();
				for(String col: cols)
					colList.add(col);
				readSet.put(inputs[0], colList);
				count++;
			}

			
			rowCount = Integer.parseInt(br.readLine());
			count = 0;
			HashMap<String, HashMap<String, String>> writeSet = new HashMap<String, HashMap<String, String>>();
			while(count < rowCount)
			{
			
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
			readLen = br.readLine();
		}
	}

	/**
	 * Method to initiate transaction
	 * @param readSet
	 * @param writeSet
	 */
	private void initiateTrans(HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
	{
		String uid = java.util.UUID.randomUUID().toString();
		MetaDataMsg msg = new MetaDataMsg(clientNode, readSet, writeSet, MetaDataMsgType.REQEUST);
		msg.setUID(uid);
		inputs.put(uid, new TransDetail(System.currentTimeMillis()));
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
		
		String uid = msg.getTransaction().getTransactionID();
		Long responseTime = System.currentTimeMillis();
		if(msg.getMsgType() == ClientOPMsgType.COMMIT){
			TransDetail transDetail = inputs.get(uid);
			transDetail.setEndTime(responseTime);
			commits.put(uid, transDetail);
			AddLogEntry("Txn "+uid+" Commited with latency "+transDetail.getLatency());
		}
		else{
			TransDetail transDetail = inputs.get(uid);
			transDetail.setEndTime(responseTime);
			aborts.put(uid, transDetail);
			AddLogEntry("Txn "+uid+" Aborted with latency "+transDetail.getLatency());
		}
		AddLogEntry("Experimental Time "+(responseTime - beginTimeStamp));
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 3)
			System.out.println("Usage: UserCient <ClientID> <port> <isNewLog> <FilePath>");

		int port = Integer.parseInt(args[1]);
		boolean isNew = Boolean.parseBoolean(args[2]);
		File inputFile = null;
		if(args[3] == null)
			inputFile = new File(args[3]);
		else
			inputFile = new File(Common.dataFile);
		UserClient client = new UserClient(args[0], port, isNew);
		new Thread(client).start();
		client.init();
	}

}
