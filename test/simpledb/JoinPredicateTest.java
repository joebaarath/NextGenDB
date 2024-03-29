package simpledb;

import org.junit.Test;

import simpledb.common.Utility;
import simpledb.execution.JoinPredicate;
import simpledb.execution.Predicate;
import simpledb.systemtest.SimpleDbTestBase;
import junit.framework.JUnit4TestAdapter;

import static org.junit.Assert.*;

public class JoinPredicateTest extends SimpleDbTestBase {

  /**
   * Unit test for JoinPredicate.filter()
   */
  @Test public void filterVaryingVals() {
    int[] vals = new int[] { -1, 0, 1 };

    for (int i : vals) {
      JoinPredicate p = new JoinPredicate(0,
          Predicate.Op.EQUALS, 0);
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      JoinPredicate p = new JoinPredicate(0,
          Predicate.Op.GREATER_THAN, 0);
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      JoinPredicate p = new JoinPredicate(0,
          Predicate.Op.GREATER_THAN_OR_EQ, 0);
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      JoinPredicate p = new JoinPredicate(0,
          Predicate.Op.LESS_THAN, 0);
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      JoinPredicate p = new JoinPredicate(0,
          Predicate.Op.LESS_THAN_OR_EQ, 0);
      assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
    }
  }

  @Test public void nextGenGetFields() {
      JoinPredicate p = new JoinPredicate(1,
              Predicate.Op.EQUALS, 3);
      System.out.println(p.getField1());
      System.out.println(p.getField2());
      assertEquals(p.getField1(), 1);
      assertEquals(p.getField2(), 3);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JoinPredicateTest.class);
  }
}

