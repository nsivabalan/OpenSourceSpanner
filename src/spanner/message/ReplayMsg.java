package spanner.message;

import java.util.ArrayList;

import spanner.common.Common.ReplayMsgType;
import spanner.node.ReplayLogEntry;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.NodeProto;

public class ReplayMsg extends MessageBase{
	private NodeProto source = null;
	private ReplayMsgType msgType = null;
	private ArrayList<ReplayLogEntry> replayEntries ;
	private int lastLogPosition;
	
	public ReplayMsg(NodeProto source, ReplayMsgType type){
		this.source = source;
		replayEntries = new ArrayList<ReplayLogEntry>();
		this.msgType = type;
	}
	
	public void setLastLogPosition(int logPosition)
	{
		this.lastLogPosition = logPosition;
	}
	
	public NodeProto getSource()
	{
		return this.source;
	}
	
	public int getLastLogPosition()
	{
		return this.lastLogPosition;
	}
	
	public ReplayMsgType getMsgType()
	{
		return this.msgType;
	}
	
	public void addLogEntry(ReplayLogEntry logEntry)
	{
		this.replayEntries.add(logEntry);
	}
	
	public ArrayList<ReplayLogEntry> getAllReplayEntries(){
		return this.replayEntries;
	}

	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("Replay Msg ");
		buffer.append("Size of entries "+replayEntries.size());
		for(ReplayLogEntry logEntry: replayEntries)
		{
			buffer.append(logEntry.getLogPosition()+":"+logEntry.getOperation()+"=");
			ElementsSetProto records = logEntry.getLogEntry();
			for(ElementProto elementProto : records.getElementsList())
			{
				buffer.append(elementProto.getRow()+":");
				for(ColElementProto colElement: elementProto.getColsList())
				{
					buffer.append(colElement.getCol()+","+colElement.getValue()+";");
				}
				buffer.append("::");
			}
			buffer.append("\n");
		}
		return buffer.toString();
	}
	
}
