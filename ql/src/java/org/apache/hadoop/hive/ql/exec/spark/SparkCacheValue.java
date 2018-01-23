package org.apache.hadoop.hive.ql.exec.spark;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by lzhang66 on 1/19/2018.
 */
public class SparkCacheValue implements Writable {
  private Writable value;
  private String inputPath;

  public  SparkCacheValue(Writable value, String inputPath) {
     this.value= value;
     this.inputPath = inputPath;
  }
  public  SparkCacheValue() {

  }

  public void setValue(Writable value) {
    this.value = value;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    value.write(dataOutput);
    dataOutput.writeUTF(inputPath);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    value.readFields(dataInput);

    inputPath = dataInput.readUTF();
  }

  public static void main(String[] args)  {
    Text one = new Text("1");
    String two = "2";
    String outputFile = "c:/sparkcachevalue.txt";
  SparkCacheValue kv = new SparkCacheValue(one,two);
    DataOutputStream dataOutput = null;
    try {
      dataOutput = new DataOutputStream(new FileOutputStream(outputFile));
      kv.write(dataOutput);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
