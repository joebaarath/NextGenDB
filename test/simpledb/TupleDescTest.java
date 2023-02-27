package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Test;

import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SimpleDbTestBase;

import static org.junit.Assert.*;
import org.junit.Assert;
import junit.framework.JUnit4TestAdapter;

public class TupleDescTest extends SimpleDbTestBase {

    /**
     * Unit test for TupleDesc.combine()
     */
    @Test
    public void combine() {
        TupleDesc td1, td2, td3;

        td1 = Utility.getTupleDesc(1, "td1");
        td2 = Utility.getTupleDesc(2, "td2");

        // test td1.combine(td2)
        td3 = TupleDesc.merge(td1, td2);
        assertEquals(3, td3.numFields());
        assertEquals(3 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 3; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertTrue(combinedStringArrays(td1, td2, td3));

        // test td2.combine(td1)
        td3 = TupleDesc.merge(td2, td1);
        assertEquals(3, td3.numFields());
        assertEquals(3 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 3; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertTrue(combinedStringArrays(td2, td1, td3));

        // test td2.combine(td2)
        td3 = TupleDesc.merge(td2, td2);
        assertEquals(4, td3.numFields());
        assertEquals(4 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 4; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertTrue(combinedStringArrays(td2, td2, td3));
    }

    /**
     * Ensures that combined's field names = td1's field names + td2's field names
     */
    private boolean combinedStringArrays(TupleDesc td1, TupleDesc td2, TupleDesc combined) {
        for (int i = 0; i < td1.numFields(); i++) {
            if (!(((td1.getFieldName(i) == null) && (combined.getFieldName(i) == null)) ||
                    td1.getFieldName(i).equals(combined.getFieldName(i)))) {
                return false;
            }
        }

        for (int i = td1.numFields(); i < td1.numFields() + td2.numFields(); i++) {
            if (!(((td2.getFieldName(i - td1.numFields()) == null) && (combined.getFieldName(i) == null)) ||
                    td2.getFieldName(i - td1.numFields()).equals(combined.getFieldName(i)))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Unit test for TupleDesc.getType()
     */
    @Test
    public void getType() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len : lengths) {
            TupleDesc td = Utility.getTupleDesc(len);
            for (int i = 0; i < len; ++i)
                assertEquals(Type.INT_TYPE, td.getFieldType(i));
        }
    }

    /**
     * Unit test for TupleDesc.nameToId()
     */
    @Test
    public void nameToId() {
        int[] lengths = new int[] { 1, 2, 1000 };
        String prefix = "test";

        for (int len : lengths) {
            // Make sure you retrieve well-named fields
            TupleDesc td = Utility.getTupleDesc(len, prefix);
            for (int i = 0; i < len; ++i) {
                assertEquals(i, td.fieldNameToIndex(prefix + i));
            }

            // Make sure you throw exception for non-existent fields
            try {
                td.fieldNameToIndex("foo");
                Assert.fail("foo is not a valid field name");
            } catch (NoSuchElementException e) {
                // expected to get here
            }

            // Make sure you throw exception for null searches
            try {
                td.fieldNameToIndex(null);
                Assert.fail("null is not a valid field name");
            } catch (NoSuchElementException e) {
                // expected to get here
            }

            // Make sure you throw exception when all field names are null
            td = Utility.getTupleDesc(len);
            try {
                td.fieldNameToIndex(prefix);
                Assert.fail("no fields are named, so you can't find it");
            } catch (NoSuchElementException e) {
                // expected to get here
            }
        }
    }

    /**
     * Unit test for TupleDesc.getSize()
     */
    @Test
    public void getSize() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len : lengths) {
            TupleDesc td = Utility.getTupleDesc(len);
            assertEquals(len * Type.INT_TYPE.getLen(), td.getSize());
        }
    }

    /**
     * Unit test for TupleDesc.numFields()
     */
    @Test
    public void numFields() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len : lengths) {
            TupleDesc td = Utility.getTupleDesc(len);
            assertEquals(len, td.numFields());
        }
    }

    @Test
    public void testEquals() {
        TupleDesc singleInt = new TupleDesc(new Type[] { Type.INT_TYPE });
        TupleDesc singleInt2 = new TupleDesc(new Type[] { Type.INT_TYPE });
        TupleDesc intString = new TupleDesc(new Type[] { Type.INT_TYPE, Type.STRING_TYPE });
        TupleDesc intString2 = new TupleDesc(new Type[] { Type.INT_TYPE, Type.STRING_TYPE });

        // .equals() with null should return false
        assertNotEquals(null, singleInt);

        // .equals() with the wrong type should return false
        assertNotEquals(singleInt, new Object());

        assertEquals(singleInt, singleInt);
        assertEquals(singleInt, singleInt2);
        assertEquals(singleInt2, singleInt);
        assertEquals(intString, intString);

        assertNotEquals(singleInt, intString);
        assertNotEquals(singleInt2, intString);
        assertNotEquals(intString, singleInt);
        assertNotEquals(intString, singleInt2);
        assertEquals(intString, intString2);
        assertEquals(intString2, intString);
    }

    // check for the behavior of getTupleDesc when invalid inputs are added
    // an exception should be thrown when a TupleDesc is created with an empty Type
    // array
    @Test(expected = Exception.class)
    public void invalidGetTupleDesc() {
        Type[] typeAr2 = new Type[] {};
        String[] fieldAr2 = { "hello", "world" };
        TupleDesc td2 = new TupleDesc(typeAr2, fieldAr2);
    }

    @Test(expected = Exception.class)
    public void invalidGetTupleDesc2() {
        Type[] typeAr2 = new Type[] {};
        TupleDesc td2 = new TupleDesc(typeAr2);
    }

    /**
     * Unit test for TupleDesc.iterator()
     */
    @Test
    public void iteratorTest() {
        String prefix = "test";

        TupleDesc td = Utility.getTupleDesc(5, prefix);
        Iterator<TupleDesc.TDItem> it = td.iterator();
        int i = 0;
        while (it.hasNext()) {
            TupleDesc.TDItem tdItem = it.next();
            assertEquals(Type.INT_TYPE, tdItem.fieldType);
            assertEquals(prefix + i, tdItem.fieldName);
            i++;
        }
        assertFalse(it.hasNext());
    }

    /**
     * Unit test for TupleDesc.getFieldName()
     */
    @Test
    public void getFieldName() {
        String prefix = "test";
        TupleDesc td = Utility.getTupleDesc(5, prefix);
        for (int i = 0; i < 5; ++i) {
            assertEquals(prefix + i, td.getFieldName(i));
        }
    }

    /**
     * Unit test for TupleDesc.getFieldName() when i is invalid
     */
    @Test(expected = NoSuchElementException.class)
    public void invalidGetFieldName() {
        int i = 9;
        TupleDesc td = Utility.getTupleDesc(5);
        td.getFieldName(i);
    }

    /**
     * Unit test for TupleDesc.getFieldType()
     */
    @Test
    public void getFieldType() {
        TupleDesc td = Utility.getTupleDesc(5);
        for (int i = 0; i < 5; i++) {
            assertEquals(Type.INT_TYPE, td.getFieldType(i));
        }
    }

    /**
     * Unit test for TupleDesc.getFieldType() when i is invalid
     */
    @Test(expected = NoSuchElementException.class)
    public void invalidGetFieldType() {
        int i = 9;
        TupleDesc td = Utility.getTupleDesc(5);
        td.getFieldType(i);
    }

    /**
     * Unit test for TupleDesc.getSize()
     */
    @Test
    public void getSizeTest() {
        TupleDesc td = Utility.getTupleDesc(5);
        assertEquals(5 * Type.INT_TYPE.getLen(), td.getSize());
    }

    /**
     * Unit test for TupleDesc.merge()
     */
    @Test
    public void mergeTest() {
        TupleDesc td1 = Utility.getTupleDesc(5);
        TupleDesc td2 = Utility.getTupleDesc(5, "test");
        TupleDesc td3 = TupleDesc.merge(td1, td2);
        assertEquals(10, td3.numFields());
        for (int i = 0; i < 5; i++) {
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
            assertEquals(Type.INT_TYPE, td3.getFieldType(i + 5));
        }
        for (int i = 0; i < 5; i++) {
            assertEquals(null, td3.getFieldName(i));
            assertEquals("test" + (i), td3.getFieldName(i + 5));
        }
    }

    /**
     * Unit test for TupleDesc.equals()
     */
    @Test
    public void equalsTest() {
        TupleDesc td1 = Utility.getTupleDesc(5);
        TupleDesc td2 = Utility.getTupleDesc(5, "test");
        TupleDesc td3 = TupleDesc.merge(td1, td2);
        TupleDesc td4 = TupleDesc.merge(td1, td2);
        TupleDesc td5 = Utility.getTupleDesc(6);

        assertTrue(td1.equals(td1));
        assertTrue(td1.equals(td2));
        assertTrue(td2.equals(td1));
        assertFalse(td1.equals(td3));
        assertFalse(td3.equals(td1));
        assertFalse(td2.equals(td3));
        assertFalse(td3.equals(td2));
        assertFalse(td1.equals(td5));
        assertTrue(td3.equals(td4));
        assertTrue(td4.equals(td3));
    }

    /**
     * Unit test for TupleDesc.hashCode()
     */
    @Test
    public void hashCodeTest() {
        TupleDesc td1 = Utility.getTupleDesc(5);
        TupleDesc td2 = Utility.getTupleDesc(5, "test");
        TupleDesc td3 = TupleDesc.merge(td1, td2);
        TupleDesc td4 = TupleDesc.merge(td1, td2);

        assertFalse(td1.hashCode() == td2.hashCode());
        assertFalse(td1.hashCode() == td3.hashCode());
        assertFalse(td2.hashCode() == td3.hashCode());
        assertTrue(td3.hashCode() == td4.hashCode());
    }

    /**
     * Unit test for TupleDesc.toString()
     */
    @Test
    public void toStringTest() {
        TupleDesc td1 = Utility.getTupleDesc(2);
        TupleDesc td2 = Utility.getTupleDesc(2, "test");
        TupleDesc td3 = TupleDesc.merge(td1, td2);

        String td1Description = String.format("%1$s(%2$s),%3$s(%4$s)", td1.getFieldType(0), td1.getFieldName(0),
                td1.getFieldType(1), td1.getFieldName(1));
        String td2Description = String.format("%1$s(%2$s),%3$s(%4$s)", td2.getFieldType(0), td2.getFieldName(0),
                td2.getFieldType(1),
                td2.getFieldName(1));
        String td3Description = String.format("%1$s(%2$s),%3$s(%4$s),%5$s(%6$s),%7$s(%8$s)", td3.getFieldType(0),
                td3.getFieldName(0), td3.getFieldType(1), td3.getFieldName(1), td3.getFieldType(2), td3.getFieldName(2),
                td3.getFieldType(3), td3.getFieldName(3));

        assertEquals(td1Description, td1.toString());
        assertEquals(td2Description, td2.toString());
        assertEquals(td3Description, td3.toString());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TupleDescTest.class);
    }
}
