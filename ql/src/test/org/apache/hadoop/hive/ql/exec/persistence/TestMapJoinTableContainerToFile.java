package org.apache.hadoop.hive.ql.exec.persistence;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by lzhang66 on 5/24/2017.
 */
public class TestMapJoinTableContainerToFile {
  private static final Object[] KEY = new Object[] {new Text("key")};
  private static final Object[] VALUE = new Object[] {new Text("value")};
  private ByteArrayOutputStream baos;
  private ObjectOutputStream out;
  private ObjectInputStream in;
  private MapJoinKeyObject key;
  private MapJoinRowContainer rowContainer;
  private MapJoinPersistableTableContainer container;
  @Before
  public void setup() throws Exception {
    key = new MapJoinKeyObject(KEY);
    rowContainer = new MapJoinEagerRowContainer();
    rowContainer.addRow(VALUE);
    baos = new ByteArrayOutputStream();
    out = new ObjectOutputStream(baos);

    container = new HashMapWrapper();
  }

  @Test
  public void serializeMapJoinPersistableTableContainer() {
    try {
      out.writeObject(container.getMetaData());
      out.writeInt(container.size());
      out.writeObject(container.entrySet());
    } catch (IOException e) {
      System.out.println(e);
    }
  }
  }
