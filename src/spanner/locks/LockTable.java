package spanner.locks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;

import spanner.common.Common;
import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.TransactionProto;


public class LockTable {

	public static String initial;
	private static File LOCKLOG;

	public static Hashtable<String, LockHolders> lockTable;
	public static String lockTableLock;

	public LockTable (String initial) {
		lockTable = new Hashtable<String, LockHolders>();
		lockTableLock = "A";

		this.initial = initial;
	}

	public LockTable(String nodeId, boolean isNew)
	{
		this("");
		createLogFile(nodeId, isNew);
	}

	private void createLogFile(String nodeId, boolean isClear) 
	{
		File lockDir = new File(Common.LockFile);
		try {
			if(!lockDir.exists())
				lockDir.mkdirs();
			LOCKLOG = new File(Common.LockFile+"/"+nodeId+"_.log");
			if(LOCKLOG.exists())
			{
				if(isClear){	
					new FileOutputStream(LOCKLOG, false).close();
				}
				else{
				}
			}
			else{
				LOCKLOG.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized boolean getReadLock (String element, String transactionId) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				writeToLockLogFile("READ_LOCK", true, transactionId, element);
				return true;
			}
		}

		LockHolders lockHolder = lockTable.get(element);


		if (lockHolder==null) {
			/*
			 * First time to read this item
			 * FIX ME
			 */
			LockHolders newLockHolder = new LockHolders(element);
			newLockHolder.getReadLockHolders().add(new Lock(transactionId, System.currentTimeMillis()));
			lockTable.put(element, newLockHolder);
			writeToLockLogFile("READ_LOCK", true, transactionId, element);
			return true; // successful

		} else {
			// if no write lock return the value
			if (lockHolder.getWriteLockHolder()==null) {

				lockHolder.getReadLockHolders().add(new Lock(transactionId, System.currentTimeMillis()));

				lockTable.put(element, lockHolder);
				writeToLockLogFile("READ_LOCK", true, transactionId, element);
				return true; // successful
			} else {

				// check if lock expired
				if (lockHolder.getWriteLockHolder().getTimestamp()<(System.currentTimeMillis()-2000)) {
					lockHolder.setWriteLockHolder(null);

					lockHolder.getReadLockHolders().add(new Lock(transactionId, System.currentTimeMillis()));

					lockTable.put(element, lockHolder);
					writeToLockLogFile("READ_LOCK", true, transactionId, element);
					return true; // successful

				} else {
					writeToLockLogFile("READ_LOCK", false, transactionId, element);
					return false; // fail
				}
			}

		}

	}

	public synchronized boolean getWriteLock (String element, String transactionId) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				writeToLockLogFile("WRITE_LOCK", true, transactionId, element);
				return true;
			}
		}


		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {

			LockHolders newLockHolder = new LockHolders(element);
			newLockHolder.setWriteLockHolder(new Lock(transactionId, System.currentTimeMillis()));
			lockTable.put(element, newLockHolder);
			writeToLockLogFile("WRITE_LOCK", true, transactionId, element);
			return true; // successful

		} else {
			//System.out.println("Write lock holder "+lockHolder.getWriteLockHolder().getTransactionId());
			//System.out.println("lock holders time stamp : "+lockHolder.getWriteLockHolder().getTimestamp()+" cur time stamp "+System.currentTimeMillis()+" difference "+(System.currentTimeMillis()- lockHolder.getWriteLockHolder().getTimestamp()));
			if ( (lockHolder.getWriteLockHolder()==null) || lockHolder.getWriteLockHolder().getTransactionId().equalsIgnoreCase(transactionId)||
					(lockHolder.getWriteLockHolder().getTimestamp()<(System.currentTimeMillis()-2000)) ) {
				lockHolder.setWriteLockHolder(new Lock(transactionId, System.currentTimeMillis()));
				lockHolder.getReadLockHolders().clear();
				lockTable.put(element, lockHolder);
				writeToLockLogFile("WRITE_LOCK", true, transactionId, element);
				return true;
			} else {
				return false;
			}


		}

	}

	public synchronized void releaseReadLock (String element, String transactionId, boolean committed) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				writeToLockLogFile("RELEASE_READ_LOCK", transactionId, element);
				return;
			}
		}

		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {
			//if (committed) throw new RuntimeException("realeasing a non existing lock "+element);
			writeToLockLogFile("LOCK_ALREADY_RELEASED", transactionId, element);
		}
		else{
			boolean containsTheElement = false;
			if(lockHolder.getReadLockHolders() != null && lockHolder.getReadLockHolders().size() > 0 ){
				containsTheElement = lockHolder.getReadLockHolders().remove(new Lock(transactionId, System.currentTimeMillis()));
				lockTable.put(element, lockHolder);
			}

			if (!containsTheElement) {
				//if (committed) throw new RuntimeException("releasing a non existing lock "+element);
				writeToLockLogFile("LOCK_ALREADY_RELEASED", transactionId, element);
			}
			else{
				writeToLockLogFile("RELEASE_READ_LOCK", transactionId, element);
			}
		}

	}

	public synchronized void releaseWriteLock (String element, String transactionId, boolean committed) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				writeToLockLogFile("RELEASE_WRITE_LOCK", transactionId, element);
				return;
			}
		}

		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {
			//if (committed) throw new RuntimeException("realeasing a non existing lock "+element);
			writeToLockLogFile("LOCK_ALREADY_RELEASED", transactionId, element);
		}

		if (lockHolder.getWriteLockHolder()==null) {
			//if (committed) throw new RuntimeException("realeasing a non existing lock "+element);
			writeToLockLogFile("LOCK_ALREADY_RELEASED", transactionId, element);
		}

		lockHolder.setWriteLockHolder(null);

		lockTable.put(element, lockHolder);
		writeToLockLogFile("RELEASE_WRITE_LOCK", transactionId, element);
	}

	public synchronized boolean isReadLockAcquired (String element, String transactionId) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				return true;
			}
		}

		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {
			return false;
		} else {
			for (Lock holder : lockHolder.getReadLockHolders()) {
				if (holder.getTransactionId().equalsIgnoreCase(transactionId)) {
					return true;
				}	
			}
		}

		return false;

	}

	public synchronized void releaseReadLocks (ElementsSetProto readSet, String uid, boolean isCommited) {

		for (ElementProto element : readSet.getElementsList()) {
			releaseReadLock(element.getRow(), uid, isCommited);
		}

	}

	public synchronized void releaseLocks(TransactionProto trans)
	{
		releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), true);
		releaseWriteLocks(trans.getWriteSet(), trans.getTransactionID(), true);
	}

	public synchronized void releaseWriteLocks (ElementsSetProto writeSet, String uid, boolean isCommited) {

		for (ElementProto element : writeSet.getElementsList()) {
			releaseWriteLock(element.getRow(), uid, isCommited);
		}

	}

	public synchronized void releaseLocksOfAborted (TransactionProto trans) {

		releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), false);
		releaseWriteLocks(trans.getWriteSet(), trans.getTransactionID(), false);

	}

	/**
	 * Method used to append content to Log file for Locks
	 * @param counter
	 * @param type
	 * @param acceptedValue
	 */
	private void writeToLockLogFile(String type, boolean isAccepted, String transId, String element)
	{
		StringBuffer buffer = new StringBuffer();
		System.out.println(type+" "+isAccepted+" "+transId+" "+element);
		if(isAccepted)
			buffer.append(System.currentTimeMillis()+": "+element+" = "+type+" acquired by "+transId);
		else
			buffer.append(System.currentTimeMillis()+": "+element+" = "+type+" rejected for "+transId);
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(LOCKLOG, true)));
			out.println(buffer.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Method used to append content to Log file for Locks
	 * @param counter
	 * @param type
	 * @param acceptedValue
	 */
	private void writeToLockLogFile(String type, String transId, String element)
	{
		StringBuffer buffer = new StringBuffer();
		System.out.println(type+" "+transId+" "+element);
		buffer.append(System.currentTimeMillis()+": "+element+" = "+type+" by "+transId);
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(LOCKLOG, true)));
			out.println(buffer.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}