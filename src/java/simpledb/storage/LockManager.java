package simpledb.storage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
public class LockManager {
    Map<PageId, RWLock> pageToLocks;
    Map<TransactionId, Set<TransactionId>> depGraph;
    Map<TransactionId, Set<PageId>> pagesHeldByTid;

    public LockManager() {
        pageToLocks = new HashMap<PageId, RWLock>();
        depGraph = new HashMap<TransactionId, Set<TransactionId>>();
        pagesHeldByTid = new HashMap<TransactionId, Set<PageId>>();
    }

    public void acquireReadLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        RWLock lock;
        synchronized (this) {
            lock = getOrCreateLock(pid);
            if (lock.heldBy(tid)) return;
            if (!lock.holders().isEmpty() && lock.isExclusive()) {
                depGraph.put(tid, lock.holders());
                if (hasDeadLock(tid)) {
                    depGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.readLock(tid);
        synchronized (this) {
            depGraph.remove(tid);
            getOrCreatePagesHeld(tid).add(pid);
        }
    }

    public void acquireWriteLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        RWLock lock;
        synchronized (this) {
            lock = getOrCreateLock(pid);
            if (lock.isExclusive() && lock.heldBy(tid))
                return;
            if (!lock.holders().isEmpty()){
                depGraph.put(tid, lock.holders());
                if (hasDeadLock(tid)) {
                    depGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.writeLock(tid);
        synchronized (this) {
            depGraph.remove(tid);
            getOrCreatePagesHeld(tid).add(pid);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!pageToLocks.containsKey(pid)) return;
        RWLock lock = pageToLocks.get(pid);
        lock.unlock(tid);
        pagesHeldByTid.get(tid).remove(pid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        if (!pagesHeldByTid.containsKey(tid)) return;
        Set<PageId> pages = pagesHeldByTid.get(tid);
        for (Object pageId: pages.toArray()) {
            releaseLock(tid, ((PageId) pageId));
        }
        pagesHeldByTid.remove(tid);
    }

    private RWLock getOrCreateLock(PageId pageId) {
        if (!pageToLocks.containsKey(pageId))
            pageToLocks.put(pageId, new RWLock());
        return pageToLocks.get(pageId);
    }

    private Set<PageId> getOrCreatePagesHeld(TransactionId tid) {
        if (!pagesHeldByTid.containsKey(tid))
            pagesHeldByTid.put(tid, new HashSet<PageId>());
        return pagesHeldByTid.get(tid);
    }

    private boolean hasDeadLock(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        Queue<TransactionId> q = new LinkedList<TransactionId>();
        visited.add(tid);
        q.offer(tid);
        while (!q.isEmpty()) {
            TransactionId head = q.poll();
            if (!depGraph.containsKey(head)) continue;
            for (TransactionId adj: depGraph.get(head)) {
                if (adj.equals(head)) continue;

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

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return pagesHeldByTid.containsKey(tid)
                && pagesHeldByTid.get(tid).contains(pid);
    }

    public Set<PageId> getPagesHeldBy(TransactionId tid) {
        if (pagesHeldByTid.containsKey(tid))
            return pagesHeldByTid.get(tid);
        return null;
    }
}

class RWLock {
    Set<TransactionId> holders;
    Map<TransactionId, Boolean> acquirers;
    boolean exclusive;
    private int readNum;
    private int writeNum;

    public RWLock() {
        holders = new HashSet<TransactionId>();
        acquirers = new HashMap<TransactionId, Boolean>();
        exclusive = false;
        readNum = 0;
        writeNum = 0;
    }

    public void readLock(TransactionId tid) {
        if (holders.contains(tid) && !exclusive) return;
        acquirers.put(tid, false);
        synchronized (this) {
            try {
                while (writeNum != 0) this.wait();
                ++readNum;
                holders.add(tid);
                exclusive = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acquirers.remove(tid);
    }

    public void writeLock(TransactionId tid) {
        if (holders.contains(tid) && exclusive) return;
        if (acquirers.containsKey(tid) && acquirers.get(tid)) return;
        acquirers.put(tid, true);
        synchronized (this) {
            try {
                if (holders.contains(tid)) {
                    while (holders.size() > 1) {
                        this.wait();
                    }
                    readUnlockWithoutNotifyingAll(tid);
                }
                while (readNum != 0 || writeNum != 0) this.wait();
                ++writeNum;
                holders.add(tid);
                exclusive = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acquirers.remove(tid);
    }

    private void readUnlockWithoutNotifyingAll(TransactionId tid) {
        if (!holders.contains(tid)) return;
        synchronized (this) {
            --readNum;
            holders.remove(tid);
        }
    }

    public void readUnlock(TransactionId tid) {
        if (!holders.contains(tid)) return;
        synchronized (this) {
            --readNum;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void writeUnlock(TransactionId tid) {
        if (!holders.contains(tid)) return;
        if (!exclusive) return;
        synchronized (this) {
            --writeNum;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void unlock(TransactionId tid) {
        if (!exclusive)
            readUnlock(tid);
        else writeUnlock(tid);
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