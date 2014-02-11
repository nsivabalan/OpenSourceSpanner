package spanner.locks;



public class Lock {
	String transactionId;
	long timestamp;
	
	public Lock (String transactionId, long timestamp) {
		this.transactionId = transactionId;
		this.timestamp = timestamp;
	}
	
	public String getTransactionId () { return transactionId; }
	public long getTimestamp () { return timestamp; }
	
	@Override
	public boolean equals (Object obj) {
		Lock lock = (Lock) obj;
		return (transactionId.equalsIgnoreCase(lock.getTransactionId()));
	}
	
}
