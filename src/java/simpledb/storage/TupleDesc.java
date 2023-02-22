package simpledb.storage;
import simpledb.common.Type;
import java.io.Serializable;
import java.util.*;
/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     * */
    private List<TDItem> tdItems;
    private Integer numFields;
    public static class TDItem implements Serializable {
        private static final long serialVersionUID = 1L;
        /**
         * The type of the field
         * */
        public final Type fieldType;
        /**
         * The name of the field
         * */
        public final String fieldName;
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }
    private static final long serialVersionUID = 1L;
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0){
            throw new IllegalArgumentException("Empty typeAr");
        } else if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("len(typeAr) does not match len(fieldAr)");
        }
        numFields = typeAr.length;
        tdItems = new ArrayList<>(numFields);
        for(int i = 0; i < numFields; i++){
            TDItem newTDItem = new TDItem(typeAr[i], fieldAr[i]);
            tdItems.add(newTDItem);
        }
    }
    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        // Using this removes need to repeat IllegalInputException
        this(typeAr, new String[typeAr.length]);
    }
    public TupleDesc(List<TDItem> tditems) {
        this.tdItems = tditems;
        this.numFields = tditems.size();
    }
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }
    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= 0 || i <= tdItems.size()){
            return tdItems.get(i).fieldName;
        }
        else{
            throw new NoSuchElementException("No Field Name for index i");
        }
    }
    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= 0 || i <= tdItems.size()){
            return tdItems.get(i).fieldType;
        }
        else{
            throw new NoSuchElementException("No Field Type for index i");
        }
    }
    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        try{
            for (int i = 0; i < tdItems.size(); i++) {
                if (name != null && tdItems.get(i).fieldName != null) {
                    if (tdItems.get(i).fieldName.equals(name)) {
                        return i;
                    }
                }
            }
        }
        catch (Exception e2){
            throw e2;
        }
        throw new NoSuchElementException("field name not found");
    }

    @@ -247,6 +251,14 @@ public int hashCode() {
     */
        public String toString() {
            // some code goes here
            if(tdItems == null || tdItems.size() == 0){
                return "";
            }
            String finalResult = tdItems.get(0).fieldType + "(" + tdItems.get(0).fieldName + ")";
            for (int i = 1; i < tdItems.size(); i++) {
                finalResult = String.join(",",finalResult,tdItems.get(i).fieldType + "(" + tdItems.get(i).fieldName + ")" );
            }

            return finalResult;
        }
    }