package spanner.locks;

import java.util.ArrayList;

public class LockHolders {
	
	private String element;
	private ArrayList<Lock> readLockHolders;
	private Lock writeLockHolder;

	public LockHolders (String element) {
		this.element = element;
		this.readLockHolders = new ArrayList<Lock>();
		this.writeLockHolder = null;
	}
	
	@Override
	public boolean equals (Object object) {
		LockHolders lockGranule = (LockHolders) object;
		return (element.equals(lockGranule.getElement()));
	}
	
	public String getElement () { return element; }
	public ArrayList<Lock> getReadLockHolders () { return readLockHolders; }
	public Lock getWriteLockHolder () { return writeLockHolder; }
	
	public void setWriteLockHolder (Lock transactionId) { 
		writeLockHolder = transactionId; 
	}
	
}
