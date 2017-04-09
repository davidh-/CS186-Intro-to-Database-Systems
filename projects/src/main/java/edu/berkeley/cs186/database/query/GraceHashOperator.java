package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;
    /* TODO: Implement the GraceHashOperator */
    private int currCount = 0;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;

    private List<DataBox> leftValues;
    private DataBox leftValue;
    private List<Record> leftList;

    private List<DataBox> rightValues;
    private DataBox rightValue;
    private List<Record> rightList;
    private int rightListIndex;


    private HashMap<DataBox, List<Record>> hashTable = new HashMap<DataBox, List<Record>>();;


    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }
      /* TODO */
      // Stage 1: Partition
      while (this.leftIterator.hasNext()) {
        this.leftRecord = this.leftIterator.next();
        this.leftValues = this.leftRecord.getValues();
        this.leftValue = this.leftValues.get(GraceHashOperator.this.getLeftColumnIndex());
        GraceHashOperator.this.addRecord(this.leftPartitions[this.leftValue.hashCode() % (numBuffers - 1)], this.leftValues);
      }
      while (this.rightIterator.hasNext()) {
        this.rightRecord = this.rightIterator.next();
        this.rightValues = this.rightRecord.getValues();
        this.rightValue = this.rightValues.get(GraceHashOperator.this.getRightColumnIndex());
        GraceHashOperator.this.addRecord(this.rightPartitions[this.rightValue.hashCode() % (numBuffers - 1)], this.rightValues);
      }

      // Stage 2: Build
      this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[0]);
      while (this.leftIterator.hasNext()) {
        this.leftRecord = this.leftIterator.next();
        this.leftValue = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
        if (!this.hashTable.containsKey(this.leftValue)) {
          this.leftList = new ArrayList<Record>();
          this.leftList.add(this.leftRecord);
          this.hashTable.put(this.leftValue, this.leftList);
        } else {
          this.hashTable.get(this.leftValue).add(this.leftRecord);
        }
      }
      this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[0]);
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        while (this.rightIterator.hasNext() && this.rightList == null) {
          this.rightRecord = this.rightIterator.next();
          this.rightValues = this.rightRecord.getValues();
          this.rightListIndex = 0;
          this.rightList = this.hashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()));
        }
        if (this.rightList != null) {
          if (this.rightListIndex < this.rightList.size()) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.rightList.get(this.rightListIndex).getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            this.rightListIndex++;
            return true;
          }
        }
        else {
          if (this.currCount < numBuffers - 2) {
            this.currCount++;
            try {
              this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currCount]);
              this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currCount]);
            } catch (Exception e){
              return false;
            }
            this.hashTable = new HashMap<DataBox, List<Record>>();
            while (this.leftIterator.hasNext()) {
              this.leftRecord = this.leftIterator.next();
              this.leftValue = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
              if (!this.hashTable.containsKey(this.leftValue)) {
                this.leftList = new ArrayList<Record>();
                this.leftList.add(this.leftRecord);
                this.hashTable.put(this.leftValue, this.leftList);
              } else {
                this.hashTable.get(this.leftValue).add(this.leftRecord);
              }
            }
          }
          else {
            return false;
          }
        }
        this.rightList = null;
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
      if (this.hasNext()) {
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
