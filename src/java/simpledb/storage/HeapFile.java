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
    int pgCount;
    int heapFileId;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.sourceFile = f;
        this.sourceTupleDesc = td;
        this.heapFileId = this.sourceFile.getAbsoluteFile().hashCode();

        if (this.heapFileMap == null){
            this.heapFileMap = new HashMap<>();
        }


        if(this.heapFileMap.containsKey(this.heapFileId)){
            throw new UnsupportedOperationException("File already exists in Heap");
        }

        this.pgCount = (int) (this.sourceFile.length() / BufferPool.getPageSize());

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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.pgCount;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    public class HeapFileIterator implements DbFileIterator {
        HeapFile heapFile;
        TransactionId tid;
        int position;
        HeapPage heapPage;
        Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        public Iterator<Tuple> findTuples(HeapPageId pid) throws TransactionAbortedException, DbException {
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(this.tid, pid,null);
            return newPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            position = 0;
            HeapPageId pid = new HeapPageId(getId(), position);
            tupleIterator = findTuples(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            try{
                if(tupleIterator == null){
                    return false;
                }
                else if (tupleIterator.hasNext()){
                    return true;
                }
                else if (position < numPages() - 1){
                    position += 1;
                    HeapPageId pid = new HeapPageId(getId(), position);
                    tupleIterator = findTuples(pid);
                    return tupleIterator.hasNext();
                }
                return false;
            }catch (Exception e){
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator == null){
                throw new NoSuchElementException();
            }
            if(tupleIterator.hasNext()){
                return tupleIterator.next();
            }
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
            position = 0;
        }

        @Override
        public void close() {
            heapPage = null;
            tid = null;
            tupleIterator = null;
            heapFile = null;
            position = 0;
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

