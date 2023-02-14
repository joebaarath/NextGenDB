package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    //todo: not sure if this is allowed
    TDItem TDItems[];

    /**
     * A help class to facilitate organizing the information of each field
     * */
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
     *        An iterator which iterates over all the field TDItems (TupleDesc Items)
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        if(TDItems == null){
            TDItems = new TDItem[]{};
        }
        return Arrays.asList(TDItems).iterator();
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
        TDItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            TDItems[i] = new TDItem(typeAr[i],fieldAr[i]);
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
        TDItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            TDItems[i] = new TDItem(typeAr[i],null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItems.length;
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
        try{
            TDItem myItem = TDItems[i];
            return myItem.fieldName;
        } catch (NoSuchElementException e){
            throw new NoSuchElementException("Index i='" + i + "' is out of bounds.");
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
        try{
            TDItem myItem = TDItems[i];
            return myItem.fieldType;
        } catch (NoSuchElementException e){
            throw new NoSuchElementException("Index i='" + i + "' is out of bounds.");

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
        NoSuchElementException noSuchElementException = new NoSuchElementException("String name='" + name + "' doesn't exist in TDItems.");
        try{
            if(name == null){
                throw noSuchElementException ;
            }

            TDItem foundItem = Arrays.stream(TDItems)
                    .filter(tdItem -> name.equals(tdItem.fieldName))
                    .findAny()
                    .orElse(null);

            //throw exception for non-existent fields
            int fieldIndex = Arrays.asList(TDItems).indexOf(foundItem);
            if(foundItem == null){
                throw noSuchElementException ;
            }
            return fieldIndex;
        }
        catch (NoSuchElementException e){
            throw noSuchElementException;
        }
        catch (Exception e2){
            throw e2;
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int bytesEstimate = 0;
        if(TDItems != null && TDItems[0] != null){
            bytesEstimate = (int) (bytesEstimate + (Math.ceil(TDItems[0].fieldType.getLen() * TDItems.length)));
        }

        return bytesEstimate;
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
        List<TDItem> myItems = new ArrayList<TDItem>();
        myItems.addAll(List.of(td1.TDItems));
        myItems.addAll(List.of(td2.TDItems));

        Type[] myTypes = new Type[myItems.size()];
        String[] myStrings = new String[myItems.size()];
        for (int i = 0; i < myItems.size() ; i++) {
//            myTypes[i]= Type.INT_TYPE;
            myTypes[i]= myItems.get(i).fieldType;
//            myStrings[i]= "Test";
            myStrings[i]= myItems.get(i).fieldName;
//            myStrings[i]= myItems.get(i).fieldName;
        }
        return new TupleDesc(myTypes,myStrings);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        try{
            if (o instanceof TupleDesc) {
                TupleDesc TupleDescObject = (TupleDesc) o;
                if( TupleDescObject.TDItems.length == this.TDItems.length){
                    for (int i = 0; i < TupleDescObject.TDItems.length; i++) {
                        if((!Objects.equals(TupleDescObject.TDItems[i].fieldName, this.TDItems[i].fieldName)) || (this.TDItems[i].fieldType != TupleDescObject.TDItems[i].fieldType)){
                            return false;
                        }
                    }
                    return true;
                }
            }
            else
            {
                return false;
            }

        } catch (Exception e)
        {
            System.out.println("Exception details: " + e.getMessage());
            throw e;
        }

        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        //todo: complete hashCode
        throw new UnsupportedOperationException("unimplemented");
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
        if(TDItems == null || TDItems.length == 0){
            return "";
        }
        String finalResult = TDItems[0].fieldType + "(" + TDItems[0].fieldName + ")";
        for (int i = 1; i < TDItems.length; i++) {
            finalResult = String.join(",",finalResult,TDItems[i].fieldType + "(" + TDItems[i].fieldName + ")" );
        }

        return finalResult;
    }


}
