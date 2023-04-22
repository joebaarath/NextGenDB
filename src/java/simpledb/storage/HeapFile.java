package simpledb.storage;

import jdk.dynalink.beans.StaticClass;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    // HeapFile to ID
    public static Map<Integer, HeapFile> heapFileMap;

    File sourceFile;
    TupleDesc sourceTupleDesc;
    int heapFileId;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.sourceFile = f;
        this.sourceTupleDesc = td;
        this.heapFileId = this.sourceFile.getAbsoluteFile().hashCode();

        if (this.heapFileMap == null){
            this.heapFileMap = new HashMap<>();
        }

        this.heapFileMap.put(this.heapFileId,this);

    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.sourceFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        if(sourceFile == null || !sourceFile.getAbsoluteFile().exists()){
            throw new UnsupportedOperationException("File does not exists");
        }
        return this.heapFileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if(sourceTupleDesc == null){
            throw new UnsupportedOperationException("File does not exists");
        }
        return sourceTupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if(pid.getTableId() != getId() || pid == null){
            throw new IllegalArgumentException("ID doesn't match in heapFile");
        }

        byte[] buffer = new byte[BufferPool.getPageSize()];
        int pageNum = pid.getPageNumber();
        int offset = pageNum * BufferPool.getPageSize();

        HeapPage heapPage = null;
        try(RandomAccessFile file = new RandomAccessFile(sourceFile, "r");){
            file.seek(offset);
            file.read(buffer, 0, buffer.length);
            heapPage = new HeapPage((HeapPageId) pid, buffer);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        try(RandomAccessFile file = new RandomAccessFile(sourceFile, "rw");){
            file.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            file.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) this.sourceFile.length() / Database.getBufferPool().getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //  To add a new tuple to a HeapFile, you will have to find a page with an empty slot.

        //  Note that it is important that the HeapFile.insertTuple() and HeapFile.deleteTuple() methods access pages
        //  using the BufferPool.getPage() method; otherwise,
        //  your implementation of transactions in the next lab will not work properly.

        List<Page> pagesToInsert = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0){
                page.insertTuple(t);
                page.markDirty(true,tid);
                pagesToInsert.add(page);
                break;
            }
        }

        //  If no such pages exist in the HeapFile, you need to create a new page and append it to the physical file
        //  on disk. You will need to ensure that the RecordID in the tuple is updated correctly.

        if(pagesToInsert.size() <= 0){
            HeapPageId heapPageId = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(newPage); //Note that you do not necessarily need to implement writePage at this point
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            pagesToInsert.add(newPage);
        }

        return pagesToInsert;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        ArrayList<Page> pagesToDelete = new ArrayList<>();
        HeapPage pageToDelete = null;

        for (int i = 0; i < numPages(); i++) {
            if(i == pageId.getPageNumber()){
                pageToDelete = (HeapPage)  Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                pagesToDelete.add(pageToDelete);
                pageToDelete.deleteTuple(t);

            }
        }

        return pagesToDelete;
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
        TransactionId tid;
        int tableId;
        int pageNum;
        int position;
        Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
            position = 0;
            this.tableId = getId();
            this.pageNum = numPages();
        }

        public Iterator<Tuple> findTuples(HeapPageId pid) throws TransactionAbortedException, DbException {
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);

            return newPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId pid = new HeapPageId(this.tableId, position);
            tupleIterator = findTuples(pid);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            tid = null;
            tupleIterator = null;
            position = 0;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null)
            {
                return null;
            }
            else if (tupleIterator.hasNext())
            {
                return tupleIterator.next();
            }
            else{
                while (position < pageNum - 1){
                    position++;
                    open();
                    if(tupleIterator == null){
                        return null;
                    }
                    if(tupleIterator.hasNext()){
                        return tupleIterator.next();
                    }
                }
                return null;
            }

        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

//        should iterate through the tuples of each page in the HeapFile
//        The iterator must use the `BufferPool.getPage()` method to access pages in the `HeapFile`.
//        This method loads the page into the buffer pool
//        and will eventually be used (in a later lab)
//        to implement locking-based concurrency control and recovery.
//        Do not load the entire table into memory on the open() call -- this will cause an out of memory error for
//        very large tables.

        return new HeapFileIterator(tid);
    }
}

