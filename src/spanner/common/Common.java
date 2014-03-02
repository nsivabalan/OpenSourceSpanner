package spanner.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import spanner.common.Common;
import spanner.common.MessageWrapper;
import com.google.gson.*;

public class Common {
	private static Properties props;
	private final static String propsFile ="config.props";

	public static String osspanner_home ;
	
	static
	{
		
		if(System.getenv("osspanner_home") != null){
			osspanner_home = System.getenv("osspanner_home");
		}
		else
			osspanner_home = System.getProperty("user.dir");
		
		if(!osspanner_home.contains("OpenSourceSpanner"))
			osspanner_home = System.getenv("HOME")+"/OpenSourceSpanner/";
		File filePath = new File(osspanner_home+"/logs/spannerlogs/");
		File paxosLog = new File(osspanner_home+"/logs/paxoslog/");
		filePath.mkdirs();
		paxosLog.mkdirs();
		try {

	        props = new Properties();
			final InputStream cfg = new FileInputStream(osspanner_home+"/src/"+propsFile);
			props.load(cfg);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static String FilePath = osspanner_home+"/logs/spannerlogs";
	public static String PaxosLog = osspanner_home+"/logs/paxoslog/";
	public static String LockFile = osspanner_home+"/logs/locks/";
	public static String dataFile = osspanner_home+"/data/input.txt";
	
	public static String tableName = "default_Table";
	
	public enum State {ACTIVE, PAUSED};
	public enum ReplayMsgType {REQEUST, RESPONSE, ACK};
	public enum LeaderMsgType {REQUEST, RESPONSE};
	public enum TransactionType{ INIT, STARTED, READINIT, READDONE, WRITEINIT, COMMIT, ABORT, PREPARE_DONE};
	public enum RequestType {PREPARE, COMMIT, ABORT};
	public enum ClientOPMsgType{READ,WRITE,READ_RESPONSE,UNLOCK, COMMIT, RELEASE_RESOURCE, WRITE_RESPONSE,ABORT};
	public enum MetaDataMsgType{REQEUST, READ, READ_RESPONSE, RESPONSE};
	public enum PaxosDetailsMsgType{ACCEPTORS, LEADER};
	public enum PaxosMsgType{PREPARE, INIT_PAXOS, ACCEPT,ACK,COMMIT,ABORT, DECIDE};
	public enum TwoPCMsgType{COMMIT, ABORT, INFO, PREPARE, ACK, RELEASE};
	public enum SiteCrashMsgType{CRASH,RECOVER};
	public enum BcastMsgType{COMMIT_ACK,ABORT_ACK};
	public enum PaxosLeaderState{PREPARE, ACCEPT, COMMIT, COMMIT_ACK, ABORT, ABORT_ACK};
	public enum PLeaderState{ACTIVE, INIT, DORMANT};
	public enum AcceptorState{ACCEPT, COMMIT, COMMIT_ACK, ABORT, ABORT_ACK};
	public enum TPCState{INIT, COMMIT, COMMIT_ACK, ABORT, ABORT_ACK};
	public static final long TRANS_TIMEOUT = 1000;
	public static final long TPC_TIMEOUT = 2000;
	public static final long TRANS_CLIENT_TIMEOUT = 4000;
	
	public static String getProperty(String str)
	{
		return props.getProperty(str); 
	}
	
	
	public static int GetQuorumSize()
	{
		return 2;
	}
	
	
	public static String getLocalAddress(int port)
	{
		return new String("tcp://127.0.0.1:"+port);
	}

	public static String getAddress(String host, int port)
	{
		return new String("tcp://"+host+":"+port);
	}
	
	//Static Functions
		public static <T> String Serialize(T message)
		{
			Gson gson = new Gson();
			return gson.toJson(message, message.getClass());
		}
		
		
		@SuppressWarnings("rawtypes")
		public static <T> T Deserialize(String json, Class className)
		{
			Gson gson = new Gson();
			return (T) gson.fromJson(json, className);
		}
		
		
		public static <T> MessageWrapper CreateMessageWrapper(T message){
			return new MessageWrapper(Common.Serialize(message), message.getClass());
		}
		
		public static Class GetClassfromString(String s) throws ClassNotFoundException
		{
			Class<?> cls = Class.forName(s);
			return cls;
		}
	
}
