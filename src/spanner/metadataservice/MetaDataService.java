package spanner.metadataservice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.zeromq.ZMQ;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import spanner.message.ClientOpMsg;
import spanner.node.Node;

public class MetaDataService extends Node{

	ZMQ.Context context = null;
	int port = -1;
	ZMQ.Socket socket = null;
	ZMQ.Socket socketPush = null;
	HashMap<String , String> shardLocator = null;
	private HashMap<String, String> addressLocator = null;
	
	

	public MetaDataService(String filePath) throws IOException 
	{
		super("MetaData");
		context = ZMQ.context(1);
		
		shardLocator = new HashMap<String, String>();
	/*	for(String shard : shardIds){
			String shardDetails = Common.getProperty(shard);
			String[] shards = shardDetails.split(";");
			String subPort = Common.getProperty(shard+"Leader");
			System.out.println(" shard "+shard+" "+Common.getProperty(shards[0]));
			shardLocator.put(shard, "tcp://"+Common.getProperty(shards[0]));
		}*/

		socket = context.socket(ZMQ.PULL);
		System.out.println(" Listening to "+Common.getLocalAddress(port));
		socket.bind(Common.getLocalAddress(port));
		this.port = port;


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
						//	ProcessClientOpMessage(message);
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
	

}
