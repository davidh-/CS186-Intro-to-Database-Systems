package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestSchema {
  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);

    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }

  public static Record createRecordWithAllTypes3() {
    List<DataBox> dataValues = new ArrayList<DataBox>();
    dataValues.add(new BoolDataBox(true));
    dataValues.add(new IntDataBox(1));
    dataValues.add(new StringDataBox("abcde", 5));
    dataValues.add(new FloatDataBox((float) 1.2));

    return new Record(dataValues);
  }

  public static Record createRecordWithAllTypes4() {
    List<DataBox> dataValues = new ArrayList<DataBox>();
    dataValues.add(new IntDataBox(1));
    dataValues.add(new BoolDataBox(true));
    dataValues.add(new StringDataBox("abc4de", 5));
    dataValues.add(new FloatDataBox((float) 1.2));

    return new Record(dataValues);
  }
  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testInvalidRecord() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = createRecordWithAllTypes4();
    Record output = schema.verify(input.getValues());

  }

  @Test
  @Category(StudentTest.class)
  public void testValidEncode() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = createRecordWithAllTypes3();
    byte[] b  = schema.encode(input);
    Record output = schema.decode(b);
    assertEquals(input, output);
  }

  @Test
  @Category(StudentTest.class)
  public void testValidDecode() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    byte[] input  = {1, 0, 0, 0, 1, 97, 98, 99, 100, 101, 63, -103, -103, -102};
    Record r = schema.decode(input);
    byte[] output = schema.encode(r);
    java.util.Arrays.equals(input, output);
  }



  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataBox>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new StringDataBox("abcde", 5));
    values.add(new IntDataBox(10));

    schema.verify(values);
  }

}
