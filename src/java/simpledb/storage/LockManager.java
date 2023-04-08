package simpledb.storage;

import java.util.*;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
public class LockManager {
    private Map<PageId, Lock> lockMap;
    private Map<TransactionId, Set<TransactionId>> dependencyGraph;
    private Map<TransactionId, Set<PageId>> pagesUnderTransaction;

    public LockManager() {
        lockMap = new HashMap<PageId, Lock>();
        dependencyGraph = new HashMap<TransactionId, Set<TransactionId>>();
        pagesUnderTransaction = new HashMap<TransactionId, Set<PageId>>();
    }

    public void getReadLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        Lock lock;
        synchronized (this) {
            lock = getLock(pid);
            if(lock.heldBy(tid)) {
                return;
            }
            if (!lock.holders().isEmpty() && lock.isExclusive()) {
                dependencyGraph.put(tid, lock.holders());
                if (isDeadLocked(tid)) {
                    dependencyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.rLock(tid);
        synchronized (this) {
            dependencyGraph.remove(tid);
            getTransactionPages(tid).add(pid);
        }
    }

    public void getWriteLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        Lock lock;
        synchronized (this) {
            lock = getLock(pid);
            if (lock.isExclusive() && lock.heldBy(tid)) {
                return;                
            }
            if (!lock.holders().isEmpty()){
                dependencyGraph.put(tid, lock.holders());
                if (isDeadLocked(tid)) {
                    dependencyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.wLock(tid);
        synchronized (this) {
            dependencyGraph.remove(tid);
            getTransactionPages(tid).add(pid);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)){
            return;
        }
        Lock lock = lockMap.get(pid);
        lock.unlock(tid);
        pagesUnderTransaction.get(tid).remove(pid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        if (!pagesUnderTransaction.containsKey(tid)){
            return;
        }
        Set<PageId> pages = pagesUnderTransaction.get(tid);
        for (Object pageId: pages.toArray()) {
            releaseLock(tid, ((PageId) pageId));
        }
        pagesUnderTransaction.remove(tid);
    }

    private Lock getLock(PageId pageId) {
        if (!lockMap.containsKey(pageId)) {
            lockMap.put(pageId, new Lock());
        }
        return lockMap.get(pageId);
    }

    private Set<PageId> getTransactionPages(TransactionId tid) {
        if (!pagesUnderTransaction.containsKey(tid)) {
            pagesUnderTransaction.put(tid, new HashSet<PageId>());
        }
        return pagesUnderTransaction.get(tid);
    }

    private boolean isDeadLocked(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        Queue<TransactionId> q = new LinkedList<TransactionId>();
        visited.add(tid);
        q.offer(tid);
        while (!q.isEmpty()) {
            TransactionId head = q.poll();
            if (!dependencyGraph.containsKey(head)) {
                continue;
            }
            for (TransactionId adj: dependencyGraph.get(head)) {
                if (adj.equals(head)) {
                    continue;
                }
                if (!visited.contains(adj)) {
                    visited.add(adj);
                    q.offer(adj);
                } else {
                    // Deadlock detected!
                    return true;
                }
            }
        }
        return false;
    }

    public boolean hasLock(TransactionId tid, PageId pid) {
        return pagesUnderTransaction.containsKey(tid) 
                && pagesUnderTransaction.get(tid).contains(pid);
    }

    public Set<PageId> getPagesUnderTransaction(TransactionId tid) {
        if (pagesUnderTransaction.containsKey(tid)) {
            return pagesUnderTransaction.get(tid);
        }
        return null;
    }
}

class Lock {
    Set<TransactionId> holders;
    Map<TransactionId, Boolean> acquirers;
    boolean exclusive;
    private int readNum;
    private int writeNum;

    public Lock() {
        holders = new HashSet<TransactionId>();
        acquirers = new HashMap<TransactionId, Boolean>();
        exclusive = false;
        readNum = 0;
        writeNum = 0;
    }

    public void rLock(TransactionId tid) {
        if (holders.contains(tid) && !exclusive) {
            return;
        }
        acquirers.put(tid, false);
        synchronized (this) {
            try {
                while (writeNum != 0) {
                    this.wait();
                }
                readNum += 1;
                holders.add(tid);
                exclusive = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acquirers.remove(tid);
    }

    public void wLock(TransactionId tid) {
        if (holders.contains(tid) && exclusive) {
            return;
        }
        if (acquirers.containsKey(tid) && acquirers.get(tid)) {
            return;
        }
        acquirers.put(tid, true);
        synchronized (this) {
            try {
                if (holders.contains(tid)) {
                    while (holders.size() > 1) {
                        this.wait();
                    }
                    bochapReadUnlock(tid);
                }
                while (readNum != 0 || writeNum != 0) {
                    this.wait();
                }
                writeNum += 1;
                holders.add(tid);
                exclusive = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acquirers.remove(tid);
    }

    private void bochapReadUnlock(TransactionId tid) {
        if (!holders.contains(tid)) {
            return;
        }
        synchronized (this) {
            readNum -= 1;
            holders.remove(tid);
        }
    }

    public void readUnlock(TransactionId tid) {
        if (!holders.contains(tid)) {
            return;
        }
        synchronized (this) {
            readNum -= 1;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void writeUnlock(TransactionId tid) {
        if (!holders.contains(tid)) {
            return;
        }
        if (!exclusive) {
            return;
        }
        synchronized (this) {
            writeNum -= 1;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void unlock(TransactionId tid) {
        if (!exclusive) {
            readUnlock(tid);
        }
        else {
            writeUnlock(tid);
        }
    }

    public Set<TransactionId> holders() {
        return holders;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public Set<TransactionId> acquirers() {
        return acquirers.keySet();
    }

    public boolean heldBy(TransactionId tid) {
        return holders().contains(tid);
    }
}