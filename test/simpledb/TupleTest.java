package simpledb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;

public class TupleTest extends SimpleDbTestBase {

    /**
     * Unit test for Tuple.getField() and Tuple.setField()
     */
    @Test
    public void modifyFields() {
        TupleDesc td = Utility.getTupleDesc(2);

        Tuple tup = new Tuple(td);
        tup.setField(0, new IntField(-1));
        tup.setField(1, new IntField(0));

        assertEquals(new IntField(-1), tup.getField(0));
        assertEquals(new IntField(0), tup.getField(1));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));

        assertEquals(new IntField(1), tup.getField(0));
        assertEquals(new IntField(37), tup.getField(1));
    }

    /**
     * Unit test for Tuple.getTupleDesc()
     */
    @Test
    public void getTupleDesc() {
        TupleDesc td = Utility.getTupleDesc(5);
        Tuple tup = new Tuple(td);
        assertEquals(td, tup.getTupleDesc());

        // extra: check for string fields as well
        TupleDesc td2 = Utility.getTupleDesc(5, "hello");
        Tuple tup2 = new Tuple(td2);
        assertEquals(td2, tup2.getTupleDesc());

        Type[] typeAr = new Type[] { Type.INT_TYPE, Type.STRING_TYPE };
        String[] fieldAr = { "hello", "world" };
        TupleDesc td3 = new TupleDesc(typeAr, fieldAr);
        Tuple tup3 = new Tuple(td3);
        assertEquals(td3, tup3.getTupleDesc());
    }

    /**
     * Unit test for Tuple.getRecordId() and Tuple.setRecordId()
     */
    @Test
    public void modifyRecordId() {
        Tuple tup1 = new Tuple(Utility.getTupleDesc(1));
        HeapPageId pid1 = new HeapPageId(0, 0);
        RecordId rid1 = new RecordId(pid1, 0);
        tup1.setRecordId(rid1);

        try {
            assertEquals(rid1, tup1.getRecordId());
        } catch (java.lang.UnsupportedOperationException e) {
            // rethrow the exception with an explanation
            throw new UnsupportedOperationException("modifyRecordId() test failed due to " +
                    "RecordId.equals() not being implemented.  This is not required for Lab 1, " +
                    "but should pass when you do implement the RecordId class.");
        }
    }

    /**
     * Unit test for Tuple.getField() and setField()
     */
    @Test
    public void getFieldTest() {
        int numFields = 5;
        int fieldToGet1 = 3;
        int fieldToGet2 = 4;
        TupleDesc td = Utility.getTupleDesc(numFields);
        Tuple tup = new Tuple(td);

        // initialize all fields to IntFields with value 20
        for (int i = 0; i < numFields; ++i) {
            tup.setField(i, new IntField(20));
        }

        // set the fieldToGet1 field to a StringField with value "hello"
        tup.setField(fieldToGet1, new StringField("hello", Type.STRING_LEN));

        // set the fieldToGet2 field to a IntField with value 50
        tup.setField(fieldToGet2, new IntField(50));

        // check that getField(fieldToGet1) returns a StringField with value "hello"
        assertEquals(new StringField("hello", Type.STRING_LEN), tup.getField(fieldToGet1));

        // check that getField(fieldToGet2) returns an IntField with value 50
        assertEquals(new IntField(50), tup.getField(fieldToGet2));
    }

    /**
     * Unit test for Tuple.getField() when index is out of bounds
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void invalidGetFieldTest() {
        int numFields = 5;
        int fieldToGet = 6;
        TupleDesc td = Utility.getTupleDesc(numFields);
        Tuple tup = new Tuple(td);

        // initialize all fields to IntFields with value 20
        for (int i = 0; i < numFields; ++i) {
            tup.setField(i, new IntField(20));
        }

        // set the fieldToGet1 field to a StringField with value "hello"
        tup.setField(fieldToGet, new StringField("hello", Type.STRING_LEN));

        // check that getField(fieldToGet1) returns a StringField with value "hello"
        assertEquals(new StringField("hello", Type.STRING_LEN), tup.getField(fieldToGet));
    }

    /**
     * Unit test for Tuple.setField() when index is out of bounds
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void invalidSetFieldTest() {
        int numFields = 5;
        TupleDesc td = Utility.getTupleDesc(numFields);
        Tuple tup = new Tuple(td);

        // initialize all fields to IntFields with value 20
        tup.setField(10, new IntField(20));
    }

    /**
     * Unit test for Tuple.toString(), assuming that the contents are delimitted
     * with a single space
     */
    @Test
    public void toStringTest() {
        int numFields = 5;
        int fieldToGet1 = 3;
        int fieldToGet2 = 4;
        TupleDesc td = Utility.getTupleDesc(numFields);
        Tuple tup = new Tuple(td);

        // initialize all fields to IntFields with value 20
        for (int i = 0; i < numFields; ++i) {
            tup.setField(i, new IntField(20));
        }

        // set the fieldToGet1 field to a StringField with value "hello"
        tup.setField(fieldToGet1, new StringField("hello", Type.STRING_LEN));

        // set the fieldToGet2 field to a IntField with value 50
        tup.setField(fieldToGet2, new IntField(50));

        // check that toString() returns the correct string
        assertEquals("20 20 20 hello 50", tup.toString());
    }

    /**
     * Unit test for Tuple.fields()
     */
    @Test
    public void fieldsTest() {
        int numFields = 5;
        int fieldToGet1 = 3;
        int fieldToGet2 = 4;
        TupleDesc td = Utility.getTupleDesc(numFields);
        Tuple tup = new Tuple(td);

        // initialize all fields to IntFields with value 20
        for (int i = 0; i < numFields; ++i) {
            tup.setField(i, new IntField(20));
        }

        // set the fieldToGet1 field to a StringField with value "hello"
        tup.setField(fieldToGet1, new StringField("hello", Type.STRING_LEN));

        // set the fieldToGet2 field to a IntField with value 50
        tup.setField(fieldToGet2, new IntField(50));

        // check that fields() returns the correct list of fields
        Iterator<Field> iterator = tup.fields();
        List<Field> fields = new ArrayList<Field>();
        while (iterator.hasNext()) {
            fields.add(iterator.next());
        }
        assertEquals(5, fields.size());
        assertEquals(new IntField(20), fields.get(0));
        assertEquals(new IntField(20), fields.get(1));
        assertEquals(new IntField(20), fields.get(2));
        assertEquals(new StringField("hello", Type.STRING_LEN), fields.get(3));
        assertEquals(new IntField(50), fields.get(4));
    }

    /**
     * Unit test for Tuple.resetTupleDesc()
     * only the tupleDesc should be changed, not the fields
     */
    @Test
    public void resetTupleDescTest() {
        int numFields = 5;
        int fieldToGet1 = 3;
        int fieldToGet2 = 4;
        TupleDesc td = Utility.getTupleDesc(numFields);
        Tuple tup = new Tuple(td);

        // initialize all fields to IntFields with value 20
        for (int i = 0; i < numFields; ++i) {
            tup.setField(i, new IntField(20));
        }

        // set the fieldToGet1 field to a StringField with value "hello"
        tup.setField(fieldToGet1, new StringField("hello", Type.STRING_LEN));

        // set the fieldToGet2 field to a IntField with value 50
        tup.setField(fieldToGet2, new IntField(50));

        // check that resetTupleDesc() returns the correct TupleDesc
        TupleDesc newTd = Utility.getTupleDesc(5, "test");
        tup.resetTupleDesc(newTd);
        assertEquals(newTd, tup.getTupleDesc());
        assertEquals(new IntField(50), tup.getField(fieldToGet2));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TupleTest.class);
    }
}
