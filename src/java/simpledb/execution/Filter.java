package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator childOp;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *              The predicate to filter tuples with
     * @param child
     *              The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.childOp = child;
        this.predicate = p;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return childOp.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        childOp.open();
        return;
    }

    public void close() {
        // some code goes here
        super.close();
        childOp.close();
        return;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOp.rewind();
        return;
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (childOp.hasNext()) {
            Tuple t = childOp.next();
            if (predicate.filter(t)) {
                return t;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { childOp };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        childOp = children[0];
    }

}
