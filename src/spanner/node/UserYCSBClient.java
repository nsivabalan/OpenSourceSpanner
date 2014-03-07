package spanner.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.TransactionalDB;

import spanner.protos.Protos.NodeProto;

public class UserYCSBClient extends TransactionalDB{
	private boolean inTxn;

	NodeProto transClient ;
	HashMap<String, ArrayList<String>> readSet = null;
	HashMap<String, HashMap<String, String>> writeSet = null;
	Long startTime = null;
	String uid = null;
	boolean isCommitted = false;
	Long experimentTimeStamp = null;
	ArrayList<String> clients = null;
	Long avgLatency = new Long(0);
	IntermediateClient client = null;
	boolean isResultObtained = false;
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

	public void init () throws DBException {

		try {
			//clients = new ArrayList<String>();
			
			/*Properties props = new Properties();
			InputStream cfg = new FileInputStream("client.info");
			props.load(cfg);
			String[] clientAddresses = ((String)props.get("clients")).split(",");
			clients.addAll(Arrays.asList(clientAddresses));
			
			System.out.println("Total clients available "+clients.size());
			int clientRandom = new Random().nextInt(clients.size());
			
			String[] hostAddress = clients.get(clientRandom).split(":");
			*/
			
			int min = 20005;
			int max = 29999;
			int port = min + (int)(Math.random() * ((max - min) + 1));
			System.out.println("Port :::: "+port);
			client = new IntermediateClient("client"+port, port , true, this);

			new Thread(client).start();
			experimentTimeStamp = System.currentTimeMillis();
			inTxn = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private void begin () {


		//System.out.println ("BEGIN YCSB txn");

		if (inTxn) {
			//System.out.println ("ALREADY IN YCSB txn");
			return;
		}
		else{
			//System.out.println ("BEG1N YCSB txn");
			readSet = new HashMap<String, ArrayList<String>>();
			writeSet = new HashMap<String, HashMap<String, String>>();

			inTxn = true;
		}
	}


	@Override
	public synchronized int commit() {

		//System.out.println ("COMMIT YCSB txn");


		boolean success = false;
		try {
			client.initiateTrans(readSet, writeSet);
			while( !isResultObtained )
			{
				//System.out.println("Waiting .");
				wait();
				//System.out.println("Waiting ...... ");
			}
			success = isCommitted;
			long rtime = System.currentTimeMillis();
			System.out.println ("EXP: "+experimentTimeStamp);
			System.out.println ("C0MM1T\t"+(rtime - experimentTimeStamp)+"\t"+(rtime - startTime));
			isResultObtained = false;
		} catch (RuntimeException | InterruptedException e) {
			e.printStackTrace();
		}


		inTxn = false;
		if (success)
			return 1;
	return -1;
}

@Override
public int rollback() {
	throw new RuntimeException("rollback() is not implemented");
}

@Override
public int setSavePoint() {
	throw new RuntimeException("setSavePoint() is not implemented");
}


@Override
public int read(String table, String key, Set<String> fields,
		HashMap<String, String> result) {

	//System.out.println ("READ YCSB txn");

	//if (aborted==true) {
	//	return 1;
	//}

	begin();

	String field = (String) fields.toArray()[0];
	int id = Integer.parseInt(field.substring(5));

	String value = null;

	double prob = Math.random();
	if (prob<0.33)
		//value = client.get("x"+id, "cf:a");

		readSet.put("x"+id, new ArrayList<String>() {{
			add("a");
		}});
	else if (prob<0.66)
		readSet.put("y"+id, new ArrayList<String>() {{
			add("a");
		}});
	else
		readSet.put("z"+id, new ArrayList<String>() {{
			add("a");
		}});

	return 1;
}


@Override
public int scan(String table, String startkey, int recordcount,
		Set<String> fields, Vector<HashMap<String, String>> result) {
	throw new RuntimeException("scan() is not implemented");
}

@Override
public int update(String table, String key, HashMap<String, String> values) {

	//System.out.println ("UPDATE YCSB txn");

	//if (aborted==true) {
	//	return -1;
	//}

	begin ();

	String field = (String) values.keySet().toArray()[0];
	String value = (String) values.values().toArray()[0];
	int id = Integer.parseInt(field.substring(5));
	value = value.replace('|', '.');
	String row;
	double prob = Math.random();
	if (prob<0.33)
		row = "x"+id;
	else if (prob<0.66)
		row = "y"+id;
	else
		row = "z"+id;
	value = "A";
	//client.put(value, row, "cf:a");
	HashMap<String, String> val = new HashMap<String, String>();
	val.put("a", value);
	writeSet.put(row, val);


	return 1;
}

@Override
public int insert(String table, String key, HashMap<String, String> values) {
	throw new RuntimeException("insert() is not implemented");
}

@Override
public int delete(String table, String key) {
	throw new RuntimeException("delete() is not implemented");
}


public synchronized void processResponse(String clientId, String uid, long startTime, boolean isCommitted)
{
	//	System.out.println("Processing Client response in YCSB client "+clientId);
	this.uid = uid;
	this.startTime = startTime;
	this.isCommitted = isCommitted;
	isResultObtained = true;
	System.out.println("Txn : "+uid+" isCommitted :: "+isCommitted);
	notifyAll();
}

@Override
public void cleanup()
{
	client.cleanup();
	System.out.println("Client thread shutdown");
}

/**
 * Method to process transaction response
 * @param msg
 */
/*private synchronized void ProcessClientResponse(ClientOpMsg msg)
	{
		AddLogEntry("Received Client Response "+msg);

		String uid = msg.getTransaction().getTransactionID();
		Long responseTime = System.currentTimeMillis();
		TransDetail transDetail = null;
		if(msg.getMsgType() == ClientOPMsgType.COMMIT){
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
		}
	}*/


}
