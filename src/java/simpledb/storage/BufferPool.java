package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    int numPages;

    private HashMap<PageId, LRUHelper> pidLRUMap;
    private LockManager lockManager;
    private Boolean steal = false;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;

        // Create an hashmap with the size of max no. of pages
        pidLRUMap = new HashMap<>(this.numPages);
//        this.lockManager = LockManager.getInstance();
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        if(perm.equals(Permissions.READ_WRITE)){
            lockManager.getLock(pid, tid, true);
        }
        else{
            lockManager.getLock(pid, tid, false);
        }

        if (pidLRUMap.containsKey(pid)) {

            // increment LRU pin count
            LRUHelper frame = pidLRUMap.get(pid);
            if (frame != null) {
                frame.pinCount += 1;
            }

            return pidLRUMap.get(pid).page;
        } else {
            HeapFile fileToRead = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            HeapPage pageToRead = (HeapPage) fileToRead.readPage(pid);

            if (pidLRUMap.size() >= numPages) {
                evictPage();
            }

            // keep track of pins
            if (pidLRUMap.get(pid) == null) {
                LRUHelper myLRU = new LRUHelper(pageToRead, 1, LRUHelper.incrementLatestUsedCount());
                pidLRUMap.put(pid, myLRU);
            }

            return pageToRead;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // These methods should call the appropriate methods in the HeapFile
        // that belong to the table being modified
        // (this extra level of indirection is needed to support
        // other types of files — like indices — in the future).

        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> insertedPages = heapFile.insertTuple(tid, t);
        for (Page page : insertedPages) {
            page.markDirty(true, tid);

            if (!pidLRUMap.containsKey(page.getId())
                    && pidLRUMap.size() >= numPages) {
                evictPage();
            }

            // update page
            LRUHelper frame = pidLRUMap.get(page.getId());
            if (frame == null) {
                frame = new LRUHelper(page, 1, LRUHelper.incrementLatestUsedCount());
            } else {
                frame.pinCount += 1;
                frame.page = page;
            }
            this.pidLRUMap.put(page.getId(), frame);
            LRUUnpin(page.getId());
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> deletedPages = heapFile.deleteTuple(tid, t);
        for (Page page : deletedPages) {
            page.markDirty(true, tid);

            // update page
            LRUHelper frame = pidLRUMap.get(page.getId());
            if (frame == null) {
                throw new DbException(
                        "Page of deleteTuple was not been loaded into LRUHelper frame. Page may not have been pinned during getPage or erroneously unpinned earlier.");
            } else {
                frame.pinCount += 1;
                frame.page = page;
            }
            this.pidLRUMap.put(page.getId(), frame);
            LRUUnpin(page.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

        // Because of the way we have implemented ScanTest.cacheTest,
        // you will need to ensure that your flushPage
        // and flushAllPages methods do no evict pages
        // from the buffer pool to properly pass this test.

        // flushAllPages should call flushPage on all pages in the BufferPool
        for (PageId pid : pidLRUMap.keySet()) {
            try {
                if (pidLRUMap.get(pid).page.isDirty() != null) {
                    flushPage(pid);
                }
            } catch (Exception e) {
                throw new IOException("Failed to flush page with PageID with tableId: " + pid.getTableId()
                        + " and pgNo of: " + pid.getPageNumber());
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        // implement discardPage() to remove a page from the buffer pool without
        // flushing it to disk.
        // We will not test discardPage() in this lab, but it will be necessary for
        // future labs.
        if (pidLRUMap.get(pid) != null) {
            pidLRUMap.remove(pid);
        }

    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // flushPage should write any dirty page to disk
        // and mark it as not dirty,
        // while leaving it in the BufferPool.
        try {
            Page pg = pidLRUMap.get(pid).page;
            if (pg.isDirty() != null) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                dbFile.writePage(pg);
                pg.markDirty(true, null);
            }
        } catch (Exception e) {
            throw new IOException("Exception during flushing of page with pageid with table id:" + pid.getTableId()
                    + " and pageNum: " + pid.getPageNumber());
        }

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        // The only method which should remove page
        // from the buffer pool is evictPage,
        // which should call flushPage on any dirty page it evicts.

        try {
            if (pidLRUMap.size() == 0) {
                throw new Exception("unexpected error during eviction with pidLRUMap size being 0");
            }

            ArrayList<PageId> pids = new ArrayList<PageId>();
            // sort the pids by lastUsed
            for (PageId pid : pidLRUMap.keySet()) {
                pids.add(pid);
            }
            // bubble sort to sort the pids by lastUsed
            for (int i = 0; i < pids.size(); i++) {
                for (int j = i + 1; j < pids.size(); j++) {
                    PageId temp = null;
                    LRUHelper framej = pidLRUMap.get(pids.get(j));
                    LRUHelper framei = pidLRUMap.get(pids.get(i));
                    if (framej.lastUsed > framei.lastUsed) {
                        temp = pids.get(i);
                        pids.set(i, pids.get(j));
                        pids.set(j, temp);
                    }
                }
            }

            PageId oldestPid = pids.get(0);
            // int oldestLastUsed = Integer.MAX_VALUE;
            // for (PageId pid : pidLRUMap.keySet()) {
            // LRUHelper frame = pidLRUMap.get(pid);
            // if (frame.pinCount == 0 &&
            // frame.lastUsed < oldestLastUsed) {
            // oldestLastUsed = frame.lastUsed;
            // oldestPid = pid;
            // }
            // }

            if (oldestPid == null) {
                throw new DbException("No available frames in buffer pool with pinCount = 0 to evict.");
            }

            Page pg = pidLRUMap.get(oldestPid).page;
            int currentPage = 0;

            // implement no steal
            if (pg.isDirty() == null) {
                pidLRUMap.remove(pg.getId());
            } else {
                // find a new page to evict
                currentPage++;
                while (currentPage < pids.size()) {
                    PageId pid = pids.get(currentPage);
                    Page page = pidLRUMap.get(pid).page;
                    if (page.isDirty() == null) {
                        pidLRUMap.remove(page.getId());
                        break;
                    }
                    currentPage++;
                }

            }
            // if (pg.isDirty() != null) {
            // this.flushPage(pg.getId());
            // }
            // pidLRUMap.remove(pg.getId());

        } catch (Exception e) {
            throw new DbException("Could not evict page");
        }

    }

    public void LRUUnpin(PageId pid) {

        // If page is not in the pool,
        // do nothing Else,
        // decrease the corresponding pincount
        // update the last updated if pin count = 0
        LRUHelper frame = pidLRUMap.get(pid);
        if (frame != null) {
            if (frame.pinCount > 0) {
                frame.pinCount -= 1;
            }
            if (frame.pinCount == 0) {
                frame.lastUsed = LRUHelper.incrementLatestUsedCount();
            }
        }

    }

}

class LRUHelper {
    int pinCount = 0;
    int lastUsed = 0;
    static int latestUsedCount = 0;
    Page page;

    LRUHelper(Page _page, int _pinCount, int _lastUsed) {
        page = _page;
        pinCount = _pinCount;
        lastUsed = _lastUsed;
    }

    static int incrementLatestUsedCount() {
        latestUsedCount += 1;
        return latestUsedCount;
    }
}
