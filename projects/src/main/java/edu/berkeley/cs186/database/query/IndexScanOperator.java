package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IndexScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataBox value;

  private int columnIndex;


  /**
   * An index scan operator.
   *
   * @param transaction the transaction containing this operator
   * @param tableName the table to iterate over
   * @param columnName the name of the column the index is on
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public IndexScanOperator(Database.Transaction transaction,
                           String tableName,
                           String columnName,
                           QueryPlan.PredicateOperator predicate,
                           DataBox value) throws QueryPlanException, DatabaseException {
    super(OperatorType.INDEXSCAN);
    this.tableName = tableName;
    this.transaction = transaction;
    this.columnName = columnName;
    this.predicate = predicate;
    this.value = value;
    this.setOutputSchema(this.computeSchema());
    columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
    this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
  }

  public String toString() {
    return "type: " + this.getType() +
        "\ntable: " + this.tableName +
        "\ncolumn: " + this.columnName +
        "\noperator: " + this.predicate +
        "\nvalue: " + this.value;
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new IndexScanIterator();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class IndexScanIterator implements Iterator<Record> {
    /* TODO: Implement the IndexScanIterator */
    private Iterator<Record> itr;
    private Record nextRecord;

    private boolean LESS_THAN;
    private boolean LESS_THAN_EQUALS;
    private boolean inner_call = false;

    public IndexScanIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.EQUALS) {
        itr = transaction.lookupKey(tableName, columnName, value);
      }
      else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN ||
              IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS) {
        itr = transaction.sortedScan(tableName, columnName);
        LESS_THAN = true;
        LESS_THAN_EQUALS = true;
      }
      else {
        itr = transaction.sortedScanFrom(tableName, columnName, value);
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.EQUALS) {
        if (itr.hasNext()) {
          if (inner_call) {
            nextRecord = itr.next();
          }
          return true;
        } else {
          return false;
        }
      }
      else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN) {
        if (itr.hasNext() && LESS_THAN) {
          nextRecord = itr.next();
          if (nextRecord.getValues().get(columnIndex).compareTo(value) >= 0) {
            LESS_THAN = false;
          }
          return true;
        } else {
          return false;
        }
      }
      else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS) {
        if (itr.hasNext() && LESS_THAN_EQUALS) {
          nextRecord = itr.next();
          if (nextRecord.getValues().get(columnIndex).compareTo(value) > 0) {
            LESS_THAN = false;
            nextRecord = null;
            return false;
          }
          return true;
        } else {
          return false;
        }
      }
      else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.GREATER_THAN) {
        if (itr.hasNext()) {
          nextRecord = itr.next();
          return true;
        } else {
          return false;
        }
      }
      else {
        if (itr.hasNext()) {
          while (itr.hasNext()) {
            nextRecord = itr.next();
            if (nextRecord.getValues().get(columnIndex).compareTo(value) > 0) {
              break;
            }
          }
          return true;
        } else {
          return false;
        }
      }

    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      /* TODO */
      inner_call = true;
      if (this.hasNext()) {
        Record r = this.nextRecord;
        this.nextRecord = null;
        inner_call = false;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
