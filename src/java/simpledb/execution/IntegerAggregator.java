package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;
import simpledb.storage.TupleDesc;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field, Integer> groupByFieldToAggregateValue;
    private HashMap<Field, Integer> groupByFieldToCount;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        // check if the Op is valid
        if (what != Op.MIN && what != Op.MAX && what != Op.SUM && what != Op.AVG && what != Op.COUNT) {
            throw new IllegalArgumentException("Invalid Op");
        } else {
            this.gbfield = gbfield;
            this.gbfieldtype = gbfieldtype;
            this.afield = afield;
            this.op = what;
            this.groupByFieldToAggregateValue = new HashMap<Field, Integer>();
            this.groupByFieldToCount = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField;
        if (gbfield != Aggregator.NO_GROUPING) {
            groupByField = tup.getField(gbfield);
        } else {
            groupByField = null;
        }

        // find the updated aggregate value
        Integer aggregateVal = ((IntField) tup.getField(afield)).getValue();

        Integer defaultVal;
        if (this.op == Op.MIN || this.op == Op.MAX) {
            defaultVal = aggregateVal;
        } else {
            defaultVal = 0;
        }

        Integer currentAggregateValue = this.groupByFieldToAggregateValue.getOrDefault(groupByField, defaultVal);

        // update the count
        Integer currentCount = this.groupByFieldToCount.getOrDefault(groupByField, 0);
        this.groupByFieldToCount.put(groupByField, currentCount + 1);

        Integer updatedAggregateValue;
        switch (this.op) {
            case AVG:
                updatedAggregateValue = currentAggregateValue + aggregateVal;
                break;
            case MAX:
                updatedAggregateValue = Math.max(currentAggregateValue, aggregateVal);
                break;
            case MIN:
                updatedAggregateValue = Math.min(currentAggregateValue, aggregateVal);
                break;
            case SUM:
                updatedAggregateValue = currentAggregateValue + aggregateVal;
                break;
            case COUNT:
                updatedAggregateValue = currentAggregateValue + 1;
                break;
            default:
                updatedAggregateValue = null;
                break;
        }

        groupByFieldToAggregateValue.put(groupByField, updatedAggregateValue);
        return;
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        // create the tupleDesc corresponding to the return type
        TupleDesc td;
        if (gbfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateVal " + op.toString() });
        } else {
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE },
                    new String[] { "groupVal", "aggregateVal " + op.toString() });
        }

        List<Tuple> tuples = new ArrayList<>();

        for (Entry<Field, Integer> groupAggregate : this.groupByFieldToAggregateValue.entrySet()) {
            Tuple groupAggregateTuple = new Tuple(td);

            Integer finalAggregateVal;
            if (op == Op.AVG) {
                finalAggregateVal = groupAggregate.getValue() / groupByFieldToCount.get(groupAggregate.getKey());
            } else {
                finalAggregateVal = groupAggregate.getValue();
            }

            if (gbfield != Aggregator.NO_GROUPING) {
                groupAggregateTuple.setField(0, groupAggregate.getKey());
                groupAggregateTuple.setField(1, new IntField(finalAggregateVal));
            } else {
                groupAggregateTuple.setField(0, new IntField(finalAggregateVal));
            }
            tuples.add(groupAggregateTuple);
        }
        return new TupleIterator(td, tuples);
    }

}
