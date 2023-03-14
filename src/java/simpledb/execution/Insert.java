package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    TransactionId tid;
    OpIterator child;
    int tableId;
    TupleDesc tupleDesc;
    boolean isFetched = false;
    int count = 0;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here

        // Insert: This operator adds the tuples it reads from its child operator to the tableid specified in its constructor.
        // It should use the BufferPool.insertTuple() method to do this.
        this.tid = t;
        this.child = child;
        this.tableId = tableId;

//        These operators return the number of affected tuples.
//        This is implemented by returning a single tuple
//        with one integer field, containing the count.

        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        isFetched = false;

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isFetched == true){
            return null;
        }
        else{
            isFetched=true;
        }

        //reset count
        count = 0;
        while (child.hasNext()){
            try {
                Tuple tuple = child.next();
                count += 1;
                Database.getBufferPool().insertTuple(this.tid, this.tableId, tuple);
            } catch (Exception e) {
                throw new DbException("Insert Operation failed during insertTuple");
            }

        }
        Tuple tupleWithCountValue = new Tuple(tupleDesc);
        tupleWithCountValue.setField(0, new IntField(count));
        return tupleWithCountValue;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
