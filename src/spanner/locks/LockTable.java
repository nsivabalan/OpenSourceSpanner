package spanner.locks;

import java.util.Hashtable;

import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;
import spanner.protos.Protos.TransactionProto;


public class LockTable {

	public static String initial;

	public static Hashtable<String, LockHolders> lockTable;
	public static String lockTableLock;

	public LockTable (String initial) {
		lockTable = new Hashtable<String, LockHolders>();
		lockTableLock = "A";

		this.initial = initial;
	}

	public synchronized static boolean getReadLock (String element, String transactionId) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				return true;
			}
		}

		LockHolders lockHolder = lockTable.get(element);


		if (lockHolder==null) {
			/*
			 * First time to read this item
			 */
			LockHolders newLockHolder = new LockHolders(element);
			newLockHolder.getReadLockHolders().add(new Lock(transactionId, System.currentTimeMillis()));
			lockTable.put(element, newLockHolder);

			return true; // successful

		} else {
			// if no write lock return the value
			if (lockHolder.getWriteLockHolder()==null) {

				lockHolder.getReadLockHolders().add(new Lock(transactionId, System.currentTimeMillis()));

				lockTable.put(element, lockHolder);

				return true; // successful
			} else {

				// check if lock expired
				if (lockHolder.getWriteLockHolder().getTimestamp()<(System.currentTimeMillis()-2000)) {
					lockHolder.setWriteLockHolder(null);

					lockHolder.getReadLockHolders().add(new Lock(transactionId, System.currentTimeMillis()));

					lockTable.put(element, lockHolder);

					return true; // successful

				} else {
					return false; // fail
				}
			}

		}

	}

	public synchronized static boolean getWriteLock (String element, String transactionId) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				return true;
			}
		}


		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {

			LockHolders newLockHolder = new LockHolders(element);
			newLockHolder.setWriteLockHolder(new Lock(transactionId, System.currentTimeMillis()));
			lockTable.put(element, newLockHolder);

			return true; // successful

		} else {

			if ( (lockHolder.getWriteLockHolder()==null) ||
					(lockHolder.getWriteLockHolder().getTimestamp()<(System.currentTimeMillis()-2000)) ) {
				lockHolder.setWriteLockHolder(new Lock(transactionId, System.currentTimeMillis()));
				lockHolder.getReadLockHolders().clear();
				lockTable.put(element, lockHolder);
				return true;
			} else {
				return false;
			}


		}

	}

	public synchronized static void releaseReadLock (String element, String transactionId, boolean committed) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				return;
			}
		}

		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {
			if (committed) throw new RuntimeException("realeasing a non existing lock "+element);
		}

		boolean containsTheElement = lockHolder.getReadLockHolders().remove(new Lock(transactionId, System.currentTimeMillis()));

		lockTable.put(element, lockHolder);

		if (!containsTheElement) {
			if (committed) throw new RuntimeException("releasing a non existing lock "+element);
		}

	}

	public synchronized static void releaseWriteLock (String element, String transactionId, boolean committed) {

		if ( !initial.equals("") ) {
			if (element.charAt(0)!=initial.charAt(0)) {
				return;
			}
		}

		LockHolders lockHolder = lockTable.get(element);

		if (lockHolder==null) {
			if (committed) throw new RuntimeException("realeasing a non existing lock "+element);
		}

		if (lockHolder.getWriteLockHolder()==null) {
			if (committed) throw new RuntimeException("realeasing a non existing lock "+element);
		}

		lockHolder.setWriteLockHolder(null);

		lockTable.put(element, lockHolder);

	}

	public synchronized static boolean isReadLockAcquired (String element, String transactionId) {

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

	public synchronized static void releaseReadLocks (ElementsSetProto readSet, String uid, boolean isCommited) {

		for (ElementProto element : readSet.getElementsList()) {
			releaseReadLock(element.getRow(), uid, isCommited);
		}
		
	}
	
	public synchronized static void releaseLocks(TransactionProto trans)
	{
		releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), true);
		releaseWriteLocks(trans.getWriteSet(), trans.getTransactionID(), true);
	}

	public synchronized static void releaseWriteLocks (ElementsSetProto writeSet, String uid, boolean isCommited) {

		for (ElementProto element : writeSet.getElementsList()) {
			releaseWriteLock(element.getRow(), uid, isCommited);
		}

	}

	public synchronized static void releaseLocksOfAborted (TransactionProto trans) {

		releaseReadLocks(trans.getReadSet(), trans.getTransactionID(), false);
		releaseWriteLocks(trans.getWriteSet(), trans.getTransactionID(), false);

	}



}