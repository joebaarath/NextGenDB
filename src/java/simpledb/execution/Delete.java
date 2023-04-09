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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    TransactionId tid;
    OpIterator child;
    TupleDesc tupleDesc;
    boolean isFetched = false;
    int count = 0;
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here

        this.tid = t;
        this.child = child;

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!isFetched) {
            int count = 0;
            try {
                while (child.hasNext()) {
                    Database.getBufferPool().deleteTuple(tid, child.next());
                    count += 1;
                }
            } catch (DbException | IOException e) {
                e.printStackTrace();
            }
            Tuple nextTuple = new Tuple(tupleDesc);
            nextTuple.setField(0, new IntField(count));
            isFetched = true;
            return nextTuple;
        } else {
            return null;
        }
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
