package spanner.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import spanner.node.Node;

import spanner.common.Common;

public class Node {
	protected String nodeId;

	protected Common.State NodeState;
	protected static Logger LOGGER = null;
	private static FileHandler logFile;



	public Node(String nodeId) throws IOException
	{
		this.nodeId = nodeId;
		this.NodeState = Common.State.ACTIVE;
		LOGGER = Logger.getLogger(nodeId);
		//Logging Specific
		logFile = new FileHandler(Common.FilePath+"/"+this.nodeId+".log", true);
		logFile.setFormatter(new SimpleFormatter());
		LOGGER.setLevel(Level.INFO); //Sets the default level if not provided.		
		LOGGER.addHandler(logFile);
		LOGGER.setUseParentHandlers(false);
	}	

	

	//Add a new log entry.
	public void AddLogEntry(String message, Level level){		
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}


	public void close()
	{
		logFile.close();
	}
	
}