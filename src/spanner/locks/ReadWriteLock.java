package spanner.locks;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

public class ReadWriteLock{
	private final Semaphore readMutex = new Semaphore(1);
	private final Semaphore writeMutex = new Semaphore(1);
	private final Set<Long> readerIds = Collections
			.synchronizedSet(new HashSet<Long>());
	private volatile Long writerId;


	public void readLock()  {

		try {
			readMutex.acquire();

			readerIds.add(Thread.currentThread().getId());
			if (readerIds.size() == 1) {
				writeMutex.acquire();
			}
		}catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			readMutex.release();
		}

	}


	public void readUnlock() {
		try {
			readMutex.acquire();
			long threadId = Thread.currentThread().getId();
			if (!readerIds.contains(threadId)) {
				throw new IllegalStateException(
						String.format(
								"The current thread with id: %d never acquired a read lock before.",
								threadId));
			}
			readerIds.remove(threadId);
			if (readerIds.size() == 0) {
				writeMutex.release();
			}
		} catch(InterruptedException ie){
			ie.printStackTrace();

		}finally {
			readMutex.release();
		}

	}


	public void writeLock() {
		try{
			writeMutex.acquire();
			writerId = Thread.currentThread().getId();
		}
		catch(InterruptedException ie)
		{
			ie.printStackTrace();
		}
	}


	public void writeUnlock() {
		Long threadId = Thread.currentThread().getId();
		if (writerId == null || !writerId.equals(threadId)) {
			throw new IllegalStateException(
					String.format(
							"The current thread with id: %d never acquired a write lock before.",
							threadId));
		}

		writeMutex.release();
	}
}