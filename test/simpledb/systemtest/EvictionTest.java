package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import org.junit.Assert;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Utility;
import simpledb.execution.Insert;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Creates a heap file with 1024*500 tuples with two integer fields each.  Clears the buffer pool,
 * and performs a sequential scan through all of the pages.  If the growth in JVM usage
 * is greater than 2 MB due to the scan, the test fails.  Otherwise, the page eviction policy seems
 * to have worked.
 */
public class EvictionTest extends SimpleDbTestBase {
    private static final long MEMORY_LIMIT_IN_MB = 5;
    private static final int BUFFER_PAGES = 16;

    @Test public void testHeapFileScanWithManyPages() throws IOException, DbException, TransactionAbortedException {
        System.out.println("EvictionTest creating large table");
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1024*500, null, null);
        System.out.println("EvictionTest scanning large table");
        Database.resetBufferPool(BUFFER_PAGES);
        long beginMem = SystemTestUtil.getMemoryFootprint();
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, f.getId(), "");
        scan.open();
        while (scan.hasNext()) {
            scan.next();
        }
        System.out.println("EvictionTest scan complete, testing memory usage of scan");
        long endMem = SystemTestUtil.getMemoryFootprint();
        long memDiff = (endMem - beginMem) / (1<<20);
        if (memDiff > MEMORY_LIMIT_IN_MB) {
            Assert.fail("Did not evict enough pages.  Scan took " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
        }
    }

//    @Test public void nextGenTestMore() throws IOException, DbException, TransactionAbortedException {
//        // Since above uses 2mb as a judge and the memory limit is 5mb, and 2mb is 2 * 1024 * 1024,
//        // this should be about 50 mb.
//        System.out.println("EvictionTest creating large table");
//        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 50*1024*1024, null, null);
//        System.out.println("EvictionTest scanning large table");
//        Database.resetBufferPool(BUFFER_PAGES);
//        long beginMem = SystemTestUtil.getMemoryFootprint();
//        TransactionId tid = new TransactionId();
//        SeqScan scan = new SeqScan(tid, f.getId(), "");
//        scan.open();
//        while (scan.hasNext()) {
//            scan.next();
//        }
//        System.out.println("EvictionTest scan complete, testing memory usage of scan");
//        long endMem = SystemTestUtil.getMemoryFootprint();
//        long memDiff = (endMem - beginMem) / (1<<20);
//        if (memDiff > MEMORY_LIMIT_IN_MB) {
//            Assert.fail("Did not evict enough pages.  Scan took " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
//        }
//    }



    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(EvictionTest.class);
    }
}
