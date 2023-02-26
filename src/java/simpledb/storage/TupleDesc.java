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
     */

    private List<TDItem> tdItems;
    private Integer numFields;

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
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
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
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
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("Empty typeAr");
        } else if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("len(typeAr) does not match len(fieldAr)");
        }

        numFields = typeAr.length;
        tdItems = new ArrayList<>(numFields);

        for (int i = 0; i < numFields; i++) {
            TDItem newTDItem = new TDItem(typeAr[i], fieldAr[i]);
            tdItems.add(newTDItem);

        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
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
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= 0 && i <= tdItems.size()) {
            return tdItems.get(i).fieldName;
        } else {
            throw new NoSuchElementException("No Field Name for index i");
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= 0 && i <= tdItems.size()) {
            return tdItems.get(i).fieldType;
        } else {
            throw new NoSuchElementException("No Field Type for index i");
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        try {
            for (int i = 0; i < tdItems.size(); i++) {
                if (name != null && tdItems.get(i).fieldName != null) {
                    if (tdItems.get(i).fieldName.equals(name)) {
                        return i;
                    }
                }
            }
        } catch (Exception e2) {
            throw e2;
        }
        throw new NoSuchElementException("field name not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tdItems) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> mergedTD = new ArrayList<>();
        mergedTD.addAll(td1.tdItems);
        mergedTD.addAll(td2.tdItems);
        return new TupleDesc(mergedTD);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc) {
            TupleDesc anotherTD = (TupleDesc) o;
            if (!(anotherTD.numFields() == this.numFields())) {
                return false;
            }
            for (int i = 0; i < numFields(); i++) {
                if (!this.tdItems.get(i).equals(anotherTD.tdItems.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Objects.hash(tdItems);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        if (tdItems == null || tdItems.size() == 0) {
            return "";
        }
        String finalResult = tdItems.get(0).fieldType + "(" + tdItems.get(0).fieldName + ")";
        for (int i = 1; i < tdItems.size(); i++) {
            finalResult = String.join(",", finalResult,
                    tdItems.get(i).fieldType + "(" + tdItems.get(i).fieldName + ")");
        }

        return finalResult;
    }
}
