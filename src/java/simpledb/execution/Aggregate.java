package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private Integer afield;
    private Integer gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        if (gfield != null) {
            return gfield;
        } else {
            return Aggregator.NO_GROUPING;
        }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield != null) {
            return child.getTupleDesc().getFieldName(gfield);
        } else {
            return null;
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        aggregator = instantiateAggregator();
        fillUpAggregatorIterator();
        iterator = aggregator.iterator();
        this.iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (child.hasNext()) {
            return this.iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        if (gfield == null) {
            Type fieldType = child.getTupleDesc().getFieldType(afield);
            Type[] typeAr = new Type[] { fieldType };

            String fieldName = child.getTupleDesc().getFieldName(afield);
            String[] stringAr = new String[] { aop.toString() + " " + fieldName };

            return new TupleDesc(typeAr, stringAr);
        } else {
            Type aFieldType = child.getTupleDesc().getFieldType(afield);
            Type gFieldType = child.getTupleDesc().getFieldType(gfield);
            Type[] typeAr = new Type[] { gFieldType, aFieldType };

            String aFieldName = child.getTupleDesc().getFieldName(afield);
            String gFieldName = child.getTupleDesc().getFieldName(gfield);

            String[] stringAr = new String[] { gFieldName, aop.toString() + " " + aFieldName };
            return new TupleDesc(typeAr, stringAr);
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

    // Utilty method to instantiate an Aggregator
    private Aggregator instantiateAggregator() {
        Type gbFieldType = this.child.getTupleDesc().getFieldType(gfield);
        if (this.child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            return new IntegerAggregator(gfield, gbFieldType, afield, aop);
        } else {
            return new StringAggregator(gfield, gbFieldType, afield, aop);
        }
    }

    // Utility method to fill up the aggregator with tuples from the child
    private void fillUpAggregatorIterator() throws DbException, TransactionAbortedException {
        while (this.child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
    }
}
