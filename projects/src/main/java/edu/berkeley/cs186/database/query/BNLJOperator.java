package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    /* TODO: Implement the BNLJIterator */
    /* Suggested Fields */
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;

    private Page[] block;
    private int blockPage;
    private int blockPagesTotal;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      /* Suggested Starter Code: get table names. */
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      /* TODO */
      this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
      this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);

      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
      this.nextRecord = null;

      this.block = new Page[numBuffers - 2];
      this.blockPage = 0;
      this.blockPagesTotal = 0;


      this.leftIterator.next();
      for(int i = 0; i < numBuffers - 2; i++) {
        if (this.leftIterator.hasNext()) {
          this.block[i] = this.leftIterator.next();
          this.blockPagesTotal++;
        }
      }
      this.leftPage = this.block[this.blockPage];
      this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
      this.leftRecord = getNextLeftRecordInBlock();


      this.rightIterator.next();
      this.rightPage = this.rightIterator.next();
      this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
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
      while (true) {
        if (this.rightRecord == null) {
          this.leftRecord = getNextLeftRecordInBlock();
          if (this.leftRecord == null) {
            try {
              if (!rightIterator.hasNext()) {
                if (leftIterator.hasNext()) {
                  this.blockPage = 0;
                  this.blockPagesTotal = 0;
                  for(int i = 0; i < numBuffers - 2; i++) {
                    if (this.leftIterator.hasNext()) {
                      this.block[i] = this.leftIterator.next();
                      this.blockPagesTotal++;
                    }
                  }
                } else {
                  return false;
                }
              }
              this.blockPage = 0;
              this.leftEntryNum = 0;
              this.leftPage = this.block[this.blockPage];
              this.rightPage = this.rightIterator.next();
              this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
              this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
              this.leftRecord = this.getNextLeftRecordInBlock();
            }
            catch (Exception e) {
              return false;
            }
          }
          this.rightEntryNum = 0;
          this.rightRecord = this.getNextRightRecordInPage();
        }
        else {
          DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            this.rightRecord = getNextRightRecordInPage();
            return true;
          }
          this.rightRecord = getNextRightRecordInPage();
        }
      }
    }

    private Record getNextLeftRecordInBlock() {
      /* TODO */
      try {
        while (this.blockPagesTotal > this.blockPage) {
          while (this.leftEntryNum < BNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
            byte b = this.leftHeader[this.leftEntryNum / 8];
            int bitOffset = 7 - (this.leftEntryNum % 8);
            byte mask = (byte) (1 << bitOffset);

            byte value = (byte) (b & mask);
            if (value != 0) {
              int entrySize = BNLJOperator.this.getEntrySize(this.leftTableName);

              int offset = BNLJOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
              byte[] bytes = this.leftPage.readBytes(offset, entrySize);

              Record toRtn = BNLJOperator.this.getSchema(this.leftTableName).decode(bytes);
              this.leftEntryNum++;
              return toRtn;
            }
            this.leftEntryNum++;
          }
          this.blockPage++;
          this.leftEntryNum = 0;
          if (this.blockPagesTotal != this.blockPage) {
            this.leftPage = this.block[this.blockPage];
            this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      /* TODO */
      try {

        while (this.rightEntryNum < BNLJOperator.this.getNumEntriesPerPage(this.rightTableName)) {
          byte b = this.rightHeader[this.rightEntryNum / 8];
          int bitOffset = 7 - (this.rightEntryNum % 8);
          byte mask = (byte) (1 << bitOffset);

          byte value = (byte) (b & mask);
          if (value != 0) {
            int entrySize = BNLJOperator.this.getEntrySize(this.rightTableName);

            int offset = BNLJOperator.this.getHeaderSize(this.rightTableName) + (entrySize * this.rightEntryNum);
            byte[] bytes = this.rightPage.readBytes(offset, entrySize);

            Record toRtn = BNLJOperator.this.getSchema(this.rightTableName).decode(bytes);
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
  }
}
