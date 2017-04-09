package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import org.omg.CORBA.INTERNAL;

import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    /* TODO: Implement the SortMergeIterator */
    private String leftTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getLeftColumnName() + "Left";
    private String rightTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getRightColumnName() + "Right";
    private Iterator<Record> leftRecordIterator;
    private Iterator<Record> rightRecordIterator;

    private Iterator<Page> leftPageIterator;
    private Iterator<Page> rightPageIterator;

    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;

    private Record markR;
    private int markN;

    private boolean reset;


    private List<Record> leftRecList = new ArrayList<Record>();
    private List<Record> rightRecList = new ArrayList<Record>();

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      /* TODO */

      this.leftRecordIterator = getLeftSource().iterator();
      this.rightRecordIterator = getRightSource().iterator();

      while(this.leftRecordIterator.hasNext()) {
        leftRecList.add(this.leftRecordIterator.next());
      }
      while(this.rightRecordIterator.hasNext()) {
        rightRecList.add(this.rightRecordIterator.next());
      }

      Collections.sort(leftRecList, new SortMergeIterator.LeftRecordComparator());
      Collections.sort(rightRecList, new SortMergeIterator.RightRecordComparator());

      SortMergeOperator.this.createTempTable(SortMergeOperator.this.getLeftSource().getOutputSchema(), leftTableName);
      for (int i = 0; i < leftRecList.size(); i++) {
        SortMergeOperator.this.addRecord(leftTableName, leftRecList.get(i).getValues());
      }

      SortMergeOperator.this.createTempTable(SortMergeOperator.this.getRightSource().getOutputSchema(), rightTableName);
      for (int i = 0; i < rightRecList.size(); i++) {
        SortMergeOperator.this.addRecord(rightTableName, rightRecList.get(i).getValues());
      }

      this.leftPageIterator = SortMergeOperator.this.getPageIterator(this.leftTableName);
      this.rightPageIterator = SortMergeOperator.this.getPageIterator(this.rightTableName);

      this.leftEntryNum = 0;
      this.rightEntryNum = 0;

      this.leftPageIterator.next();
      this.leftPage = this.leftPageIterator.next();

      this.rightPageIterator.next();
      this.rightPage = this.rightPageIterator.next();

      this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTableName, this.leftPage);
      this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, this.rightPage);

      this.nextRecord = null;
      this.leftRecord = getNextLeftRecordInPage();
      this.rightRecord = getNextRightRecordInPage();
    }

    /**
    * Checks if there are more record(s) to yield
    *
    * @return true if this iterator has another record to yield, otherwise false
    */
    public boolean hasNext() {
      /* TODO */
      if (this.nextRecord != null) {
        return true;
      }
      while (true){
        if (this.leftRecord == null) {
          if (this.leftPageIterator.hasNext()) {
            this.leftPage = this.leftPageIterator.next();
            this.leftRecord = getNextLeftRecordInPage();
          }
          else {
            return false;
          }
        }
        if (this.rightRecord == null) {
          if (reset == true) {
            this.rightRecord = markR;
            this.rightEntryNum = markN;
            this.leftRecord = getNextLeftRecordInPage();
            reset = false;
            continue;
          }
          else if (this.rightPageIterator.hasNext()) {
            this.rightPage = this.rightPageIterator.next();
            this.rightRecord = getNextRightRecordInPage();
          } else {
            return false;
          }
        }
        DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
        DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
        if  (leftJoinValue.compareTo(rightJoinValue) < 0) {
          this.leftRecord = getNextLeftRecordInPage();
          continue;
        }
        else if (leftJoinValue.compareTo(rightJoinValue) > 0){
          this.rightRecord = getNextRightRecordInPage();
          continue;
        }
        else {
          if (reset == false) {
            markR = this.rightRecord;
            markN = this.rightEntryNum;
            reset = true;
          }
          if (leftJoinValue.compareTo(rightJoinValue) == 0 ) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            this.rightRecord = getNextRightRecordInPage();
            return true;
          }
        }
      }
    }


    private Record getNextLeftRecordInPage() {
      /* TODO */
      try {
        while (this.leftEntryNum < SortMergeOperator.this.getNumEntriesPerPage(leftTableName)) {
          byte b = this.leftHeader[this.leftEntryNum / 8];
          int bitOffset = 7 - (this.leftEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);

          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = SortMergeOperator.this.getEntrySize(this.leftTableName);

            int offset = SortMergeOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
            byte[] bytes = this.leftPage.readBytes(offset, entrySize);

            Record toRtn = SortMergeOperator.this.getSchema(this.leftTableName).decode(bytes);
            this.leftEntryNum++;
            return toRtn;
          }
          this.leftEntryNum++;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      /* TODO */
      try {
        while (this.rightEntryNum < SortMergeOperator.this.getNumEntriesPerPage(this.rightTableName)) {
          byte b = this.rightHeader[this.rightEntryNum / 8];
          int bitOffset = 7 - (this.rightEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);

          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = SortMergeOperator.this.getEntrySize(this.rightTableName);

            int offset = SortMergeOperator.this.getHeaderSize(this.rightTableName) + (entrySize * this.rightEntryNum);
            byte[] bytes = this.rightPage.readBytes(offset, entrySize);

            Record toRtn = SortMergeOperator.this.getSchema(this.rightTableName).decode(bytes);
            this.rightEntryNum++;
            return toRtn;
          }
          this.rightEntryNum++;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
