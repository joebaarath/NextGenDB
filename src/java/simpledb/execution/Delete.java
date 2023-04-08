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
                Tuple tupleToDelete = this.child.next();
                Database.getBufferPool().deleteTuple(this.tid, tupleToDelete);
                count += 1;
            } catch (Exception e) {
                throw new DbException("Deletion Operation failed.");
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
