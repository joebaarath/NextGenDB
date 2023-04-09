package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Lock {
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

    public boolean heldBy(TransactionId tid) {
        return holders().contains(tid);
    }

    public boolean isExclusive() {
        return exclusive;
    }

}
