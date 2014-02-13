package spanner.node;

import spanner.protos.Protos.ElementsSetProto;

public class ReplayLogEntry {

	int logPosition;
	private String operation;
	private ElementsSetProto logEntry;
	
	public ReplayLogEntry(int logPosition, String operation, ElementsSetProto logEntry)
	{
		this.logPosition = logPosition;
		this.operation = operation;
		this.logEntry = logEntry;
	}

	public int getLogPosition() {
		return logPosition;
	}

	public String getOperation() {
		return operation;
	}

	public ElementsSetProto getLogEntry() {
		return logEntry;
	}
	
	
}
