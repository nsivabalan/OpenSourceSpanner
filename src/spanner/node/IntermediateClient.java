package spanner.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

public class IntermediateClient extends Node implements Runnable{

	ZMQ.Context context = null;
	ZMQ.Socket subscriber = null;
	int port = -1;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;

	
	private long beginTimestamp;
	private long experimentTimestamp;
	
	NodeProto transClient ;
	NodeProto clientNode;
	AtomicInteger obtainedResults = null;
	AtomicInteger totalNoofCommits = null;
	AtomicLong avgLatency = null;
	int totalNoOfInputs = 0;
	HashMap<String, TransDetail> commits = null;
	HashMap<String, TransDetail> aborts = null;
	ConcurrentHashMap<String, TransDetail> inputs = null;
	HashMap<String, ArrayList<String>> readSet = null;
	HashMap<String, HashMap<String, String>> writeSet = null;
	UserYCSBClient ycsbClient = null;
	Long beginTimeStamp = null;
	public IntermediateClient(String clientID, int port, boolean isNew, UserYCSBClient ycsbClient) throws IOException
	{
		super(clientID, isNew);
		this.ycsbClient = ycsbClient;
		context = ZMQ.context(1);
		String host = Common.getProperty("client");
		clientNode = NodeProto.newBuilder().setHost("54.212.59.169").setPort(port).build();
		socket = context.socket(ZMQ.PULL);
		AddLogEntry("Listening to messages at "+Common.getLocalAddress(port));
		socket.bind("tcp://*:"+port);
		this.port = port;
		/*String[] transcli = Common.getProperty("transClient").split(":");
		if(transcli[0].equalsIgnoreCase("localhost"))
			transClient = NodeProto.newBuilder().setHost("127.0.0.1").setPort(Integer.parseInt(transcli[1])).build();
		else
			transClient = NodeProto.newBuilder().setHost(transcli[0]).setPort(Integer.parseInt(transcli[1])).build();*/
		String[] transclients = Common.getProperty("transClients").split(",");
		
		int randomTC = new Random().nextInt(transclients.length)-1;
		System.out.println("Random "+randomTC+" "+(new Random().nextInt(transclients.length)-1)+" "+(new Random().nextInt(transclients.length)-1));
		
		String[] hostAddress = transclients[randomTC].split(":");
		AddLogEntry("Chosen Transactional Client "+hostAddress[0]+":"+hostAddress[1]);
		transClient = NodeProto.newBuilder().setHost(hostAddress[0]).setPort(Integer.parseInt(hostAddress[1])).build();
		beginTimeStamp = System.currentTimeMillis();
		obtainedResults = new AtomicInteger(0);
		totalNoofCommits = new AtomicInteger(0);
		avgLatency = new AtomicLong(0);
		commits = new HashMap<String, TransDetail>();
		aborts = new HashMap<String, TransDetail>();
		inputs = new ConcurrentHashMap<String,TransDetail>();
		experimentTimestamp = System.currentTimeMillis();
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
	//	System.out.println("Thread interrputed");
		socket.close();
		context.term();
		//System.out.println("Done ..... ");
	}

	public void cleanup()
	{
		//System.out.println("Shutting down the thread ");
		Thread.currentThread().interrupt();
		//System.out.println("Successfully shut down");
	}
	
	/**
	 * Method to initiate transaction
	 * @param readSet
	 * @param writeSet
	 */
	public synchronized void initiateTrans(HashMap<String, ArrayList<String>> readSet, HashMap<String, HashMap<String, String>> writeSet)
	{
		String uid = java.util.UUID.randomUUID().toString();
		MetaDataMsg msg = new MetaDataMsg(clientNode, readSet, writeSet, MetaDataMsgType.REQEUST);
		msg.setUID(uid);
		inputs.put(uid, new TransDetail(System.currentTimeMillis()));
		totalNoOfInputs++;
		AddLogEntry("Sent Txn "+uid+" with startTime "+inputs.get(uid).getStartTime());
		sendMetaDataMsg(msg);
	}

	/**
	 * Method to send msg to MDS
	 * @param msg
	 */
	private synchronized void sendMetaDataMsg(MetaDataMsg msg)
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
	private synchronized void ProcessClientResponse(ClientOpMsg msg)
	{
		AddLogEntry("Received Client Response "+msg);
		
		String uid = msg.getTransaction().getTransactionID();
		Long responseTime = System.currentTimeMillis();
		
		TransDetail transDetail = inputs.get(uid);
		
		if(msg.getMsgType() == ClientOPMsgType.COMMIT){
		//	System.out.println("Making a call to ycsb method");
			ycsbClient.processResponse(nodeId, uid, transDetail.getStartTime(), true);
			//System.out.println("done with the call");
		}
		else{
			//System.out.println("Making a call to ycsb method");
			//System.out.println(uid+" .... "+transDetail.getStartTime());
			ycsbClient.processResponse(nodeId, uid, transDetail.getStartTime(), false);
		//	System.out.println("done with the call");
		}
		
		
		/*if(msg.getMsgType() == ClientOPMsgType.COMMIT){
			transDetail = inputs.get(uid);
			transDetail.setEndTime(responseTime);
			commits.put(uid, transDetail);
			totalNoofCommits.incrementAndGet();
			
			AddLogEntry("Txn "+uid+" Commited with latency "+transDetail.getLatency());
		}
		else{
			transDetail = inputs.get(uid);
			transDetail.setEndTime(responseTime);
			aborts.put(uid, transDetail);
			AddLogEntry("Txn "+uid+" Aborted with latency "+transDetail.getLatency());
		}
		AddLogEntry("Experimental Time "+(responseTime - beginTimeStamp));
		obtainedResults.incrementAndGet();
		avgLatency.set((avgLatency.get() + transDetail.getLatency())/obtainedResults.get());
		if(obtainedResults.get() == totalNoOfInputs){
			AddLogEntry("Obtained all results. Avg Latency "+avgLatency.get());
			Thread.interrupted();
		}*/
	}

	/*public static void main(String[] args) throws IOException, ClassNotFoundException {

		if(args.length != 3)
			System.out.println("Usage: UserCient <ClientID> <port> <isNewLog>");

		int port = Integer.parseInt(args[1]);
		boolean isNew = Boolean.parseBoolean(args[2]);
		File inputFile = new File(Common.dataFile);
		UserYCSBClient client = new UserYCSBClient(args[0], port, isNew, inputFile);
		new Thread(client).start();
		client.init();
	}*/

}
