package spanner.common;

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
	private final static String propsFile ="src/config.props";
	
	static
	{
        props = new Properties();
		try {
			final InputStream cfg = new FileInputStream(propsFile);
			props.load(cfg);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String FilePath = "/home/sivabalan/spannerlogs";
	public static String PaxosLog = "/home/sivabalan/paxoslog/";
	
	public static String tableName = "default_Table";
	
	public enum State {ACTIVE, PAUSED};
	public enum TransactionType{ STARTED, READINIT, READDONE, WRITEINIT, COMMIT, ABORT};
	public enum RequestType {PREPARE, COMMIT, ABORT};
	public enum ClientOPMsgType{READ,WRITE,READ_RESPONSE,UNLOCK, COMMIT, RELEASE_RESOURCE, WRITE_RESPONSE,ABORT};
	public enum MetaDataMsgType{REQEUST, RESPONSE};
	public enum PaxosDetailsMsgType{ACCEPTORS, LEADER};
	public enum PaxosMsgType{PREPARE, INIT_PAXOS, ACCEPT,ACK,COMMIT,ABORT, DECIDE};
	public enum TwoPCMsgType{COMMIT, ABORT, INFO, PREPARE, ACK};
	public enum SiteCrashMsgType{CRASH,RECOVER};
	public enum BcastMsgType{COMMIT_ACK,ABORT_ACK};
	public enum PaxosLeaderState{PREPARE, ACCEPT, COMMIT, COMMIT_ACK, ABORT, ABORT_ACK};
	public enum PLeaderState{ACTIVE, INIT, DORMANT};
	public enum AcceptorState{ACCEPT, COMMIT, COMMIT_ACK, ABORT, ABORT_ACK};
	public enum TPCState{INIT, COMMIT, COMMIT_ACK, ABORT, ABORT_ACK};
	
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
