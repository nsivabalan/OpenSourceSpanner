package spanner.node;

import java.io.File;
import java.io.FileOutputStream;
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
	private static FileHandler logFileHandler;
	private static File logFile;

	public Node(String nodeId, boolean isNew) throws IOException
	{
		this.nodeId = nodeId;
		this.NodeState = Common.State.ACTIVE;
		LOGGER = Logger.getLogger(nodeId);
		//Logging Specific
		
		logFile = new File(Common.FilePath+"/"+this.nodeId+".log");
		if(isNew)
			new FileOutputStream(logFile, false).close();
		
		logFileHandler = new FileHandler(Common.FilePath+"/"+this.nodeId+".log", true);
		logFileHandler.setFormatter(new SimpleFormatter());
		LOGGER.setLevel(Level.INFO); //Sets the default level if not provided.		
		LOGGER.addHandler(logFileHandler);
		LOGGER.setUseParentHandlers(false);
	}	

	/**
	 * Method to append content to Log with custom Log Level
	 * @param message
	 * @param level
	 */
	public void AddLogEntry(String message, Level level){	
		System.out.println(message);
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}

	/**
	 * Method  to append content to Log with defualt Log Level(INFO)
	 * @param message
	 */
	public void AddLogEntry(String message){	
		AddLogEntry(message, Level.INFO);		
	}

	
}