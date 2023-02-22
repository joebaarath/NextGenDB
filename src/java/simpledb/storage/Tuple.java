package simpledb.storage;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId recordId;
    private List<Field> fields;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.recordId = null;
        this.fields = Arrays.asList(new Field[td.numFields()]);
    }
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }
    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }
    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }
    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here

        if(i >= 0  && i < this.fields.size()){
            this.fields.set(i,f);
        }
    }

    /**
     @@ -92,8 +95,16 @@ public Field getField(int i) {
     */
    public String toString() {
        // some code goes here
        // System.out.println(this.fields.toString());
        if(fields == null || fields.size() == 0){
            return "";
        }
        String finalResult = fields.get(0).toString();
        for (int i = 0; i < fields.size(); i++) {
            finalResult = String.join(" ",finalResult,fields.get(i).toString() );
        }

        return finalResult;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return this.fields.iterator();
    }
    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
    }
}