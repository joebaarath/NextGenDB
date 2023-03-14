package simpledb;

import org.junit.Test;

import simpledb.common.Utility;
import simpledb.execution.Predicate;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.systemtest.SimpleDbTestBase;
import junit.framework.JUnit4TestAdapter;

import static org.junit.Assert.*;

public class PredicateTest extends SimpleDbTestBase{

  /**
   * Unit test for Predicate.filter()
   */
  @Test public void filter() {
    int[] vals = new int[] { -1, 0, 1 };

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN,
          TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN_OR_EQ,
          TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.LESS_THAN,
          TestUtil.getField(i));
      assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.LESS_THAN_OR_EQ,
          TestUtil.getField(i));
      assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
    }
  }

  @Test public void nextGenGetFieldTest() {
    Predicate p0 = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
    Predicate p1 = new Predicate(0, Predicate.Op.LESS_THAN_OR_EQ, TestUtil.getField(0));
    Predicate p2 = new Predicate(0, Predicate.Op.GREATER_THAN, TestUtil.getField(0));
    assertEquals(p0.getOp(), Predicate.Op.EQUALS);
    assertEquals(p1.getOp(), Predicate.Op.LESS_THAN_OR_EQ);
    assertNotEquals(p2.getOp(), Predicate.Op.EQUALS);
    assertEquals(p2.getOp(), Predicate.Op.GREATER_THAN);
  }

  @Test public void nextGenGetOpTest() {
    int[] vals = new int[]{-1, 0, 1};

    for (int i : vals) {
      Predicate p = new Predicate(i, Predicate.Op.EQUALS, TestUtil.getField(0));
      assertEquals(p.getField(),i);
    }
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PredicateTest.class);
  }
}

