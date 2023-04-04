package simpledb.storage;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {

    // To implement a simple timeout policy
    // Timeout is 1 second
    // private static LockManager lockManager = null;
    public static final long TIMEOUT = 1000;
    private Map<PageId, ArrayList<Lock>> lockMap;
    private Map<TransactionId, Set<PageId>> pagesUnderTransaction;

    public LockManager() {
        lockMap = new HashMap<PageId, ArrayList<Lock>>();
        pagesUnderTransaction = new HashMap<TransactionId, Set<PageId>>();
    }

    // public static LockManager getInstance(){
    // if(lockManager == null){
    // lockManager = new LockManager();
    // }
    // return lockManager;
    // }
    public synchronized boolean availableLock(PageId lockedPID, TransactionId lockedTID, boolean isExclusive) {
        ArrayList<Lock> lockEntries = lockMap.get(lockedPID);

        // Changed from isEmpty() to == null
        if (lockEntries == null) {
            lockMap.put(lockedPID, new ArrayList<Lock>());
            return true;
        }

        boolean toLock = false;
        Lock lockToRemove = null;
        Integer lastChecked = -1;

        for (int i = 0; i < lockEntries.size(); i++) {
            Lock lock = lockEntries.get(i);
            if (lock.tid.equals(lockedTID)) {
                if (lock.isGranted) {
                    if (isExclusive && !lock.isExclusive) {
                        toLock = true;
                        lockToRemove = lock;
                    } else {
                        return true;
                    }
                }
            } else {
                if (!lock.isExclusive && lock.isGranted) {
                    lastChecked = i;
                }

                if (lock.isExclusive && lock.isGranted) {
                    return false;
                }
            }
        }

        if (toLock) {
            if (lastChecked > -1) {
                lockEntries.remove(lockToRemove);
                if (lastChecked != lockEntries.size()) {
                    lockEntries.add(lastChecked + 1, new Lock(true, false, lockedTID));
                }

                else {
                    lockEntries.add(new Lock(true, false, lockedTID));
                }
                return false;
            }
            return true;
        }

        if (!lockEntries.isEmpty() && isExclusive) {
            return false;
        }
        return true;
    }

    public void getLock(PageId pid, TransactionId tid, boolean isExclusive) throws TransactionAbortedException {
        synchronized (this.lockMap) {
            ArrayList<Lock> lockEntries = lockMap.get(pid);
            // If the lock is available
            if (lockEntries != null) {
                if (!availableLock(pid, tid, isExclusive)) {
                    Lock newLock = new Lock(isExclusive, false, tid);
                    if (!lockEntries.contains(newLock)) {
                        lockEntries.add(newLock);

                    }
                } else {
                    Lock newLock = new Lock(isExclusive, true, tid);
                    if (!lockEntries.contains(newLock)) {
                        lockEntries.add(newLock);
                        Set<PageId> pages = pagesUnderTransaction.get(tid);
                        if (pages == null) {
                            pages = new HashSet<PageId>();
                        }
                        pages.add(pid);
                        pagesUnderTransaction.put(tid, pages);
                    }
                    return;
                }
            }
        }

        long checkpoint = System.currentTimeMillis();
        while (!availableLock(pid, tid, isExclusive)) {
            try {
                Thread.sleep(1);
                long now = System.currentTimeMillis();
                if (now - checkpoint >= TIMEOUT) {
                    throw new TransactionAbortedException();
                }
            } catch (InterruptedException e) {
            }
        }

        // lock is free! grab it.
        synchronized (this.lockMap) {
            lockMap.get(pid).add(new Lock(isExclusive, true, tid));
            Set<PageId> pages = pagesUnderTransaction.get(tid);
            if (pages == null) {
                pages = new HashSet<PageId>();
            }
            pages.add(pid);
            pagesUnderTransaction.put(tid, pages);
        }
    }

    // For unsafeReleasePage
    // Release lock for a transaction
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        ArrayList<Lock> lockEntries = lockMap.get(pid);

        for (Lock lockToRelease : lockEntries) {
            if (lockToRelease.tid.equals(tid)) {
                lockEntries.remove(lockToRelease);
                pagesUnderTransaction.get(tid).remove(pid);
                break;
            }
        }
    }

    // For transactionComplete
    // Release all locks for a particular transaction
    public void releaseAllLocks(TransactionId tid) {
        for (PageId pid : lockMap.keySet()) {
            ArrayList<Lock> lockEntries = lockMap.get(pid);

            HashSet<Lock> toRemove = new HashSet<Lock>();
            for (Lock lockToRelease : lockEntries) {
                if (lockToRelease.tid.equals(tid)) {
                    toRemove.add(lockToRelease);
                }
            }

            for (Lock lockToRelease : toRemove) {
                lockEntries.remove(lockToRelease);
            }
            pagesUnderTransaction.get(tid).clear();
        }
    }

    // For holdLocks
    public boolean holdsLock(TransactionId tid, PageId pid) {
        ArrayList<Lock> lockEntries = lockMap.get(pid);

        for (Lock lock : lockEntries) {
            if (lock.tid.equals(tid)) {
                return true;
            }
        }
        return false;
    }

    public Set<PageId> getPagesUnderTransaction(TransactionId tid) {
        if (pagesUnderTransaction.containsKey(tid))
            return pagesUnderTransaction.get(tid);
        return null;
    }

    // For 2.4
    private class Lock {
        boolean isExclusive;
        boolean isGranted;
        final TransactionId tid;

        private Lock(boolean isExclusive, boolean isGranted, TransactionId tid) {
            this.tid = tid;
            this.isExclusive = isExclusive;
            this.isGranted = isGranted;
        }
    }
}
