package org.apache.hadoop.hive.ql.exec;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.junit.Test;


/**
 * Created by lzhang66 on 12/12/2017.
 */
public class TestOperator2 extends TestCase {

  // this is our row to test expressions on
  protected InspectableObject[] r;

  @Override
  protected void setUp() {
    r = new InspectableObject[5];
    ArrayList<String> names = new ArrayList<String>(3);
    names.add("col0");
    names.add("col1");
    names.add("col2");
    ArrayList<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>(
      3);
    objectInspectors
      .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    objectInspectors
      .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    objectInspectors
      .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    for (int i = 0; i < 5; i++) {
      ArrayList<String> data = new ArrayList<String>();
      data.add("" + i);
      data.add("" + (i + 1));
      data.add("" + (i + 2));
      try {
        r[i] = new InspectableObject();
        r[i].o = data;
        r[i].oi = ObjectInspectorFactory.getStandardStructObjectInspector(
          names, objectInspectors);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void testMapOperator() throws Throwable {
    try {
      System.out.println("Testing Map Operator");
      // initialize configuration
      JobConf hconf = new JobConf(TestOperators.class);
      hconf.set(MRJobConfig.MAP_INPUT_FILE, "hdfs:///testDir/testFile");
      IOContextMap.get(hconf).setInputPath(
        new Path("hdfs:///testDir/testFile"));

      // initialize pathToAliases
      ArrayList<String> aliases = new ArrayList<String>();
      aliases.add("a");
//      aliases.add("b");
      LinkedHashMap<Path, ArrayList<String>> pathToAliases = new LinkedHashMap<>();
      pathToAliases.put(new Path("hdfs:///testDir"), aliases);

      // initialize pathToTableInfo
      // Default: treat the table as a single column "col"
      TableDesc td = Utilities.defaultTd;
      PartitionDesc pd = new PartitionDesc(td, null);
      LinkedHashMap<Path, org.apache.hadoop.hive.ql.plan.PartitionDesc> pathToPartitionInfo =
        new LinkedHashMap<>();
      pathToPartitionInfo.put(new Path("hdfs:///testDir"), pd);

      // initialize aliasToWork
      CompilationOpContext ctx = new CompilationOpContext();

      TableScanOperator tb1 = new TableScanOperator(ctx);

      initilizeOpTree(tb1,ctx);
      LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork =
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
      aliasToWork.put("a", tb1);


      // initialize mapredWork
      MapredWork mrwork = new MapredWork();
      mrwork.getMapWork().setPathToAliases(pathToAliases);
      mrwork.getMapWork().setPathToPartitionInfo(pathToPartitionInfo);
      mrwork.getMapWork().setAliasToWork(aliasToWork);

      // get map operator and initialize it
      MapOperator mo = new MapOperator(new CompilationOpContext());
      mo.initializeAsRoot(hconf, mrwork.getMapWork());
      mo.getChildOperators().remove(0);
      int num = mo.getChildOperators().size();  // num =4

//      Text tw = new Text();
//      InspectableObject io1 = new InspectableObject();
//      for (int i = 0; i < 5; i++) {
//        String answer = "[[" + i + ", " + (i + 1) + ", " + (i + 2) + "]]";
//
//        tw.set("" + i + "\u0001" + (i + 1) + "\u0001" + (i + 2));
//        mo.process(tw);
//        tb1.retrieve(io1);
//        System.out.println("io1.o.toString() = " + io1.o.toString());
//
//        System.out.println("answer.toString() = " + answer.toString());
//        assertEquals(answer.toString(), io1.o.toString());
//
//      }
//
//      System.out.println("Map Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw (e);
    }
  }

  private void initilizeOpTree(TableScanOperator tb1, CompilationOpContext ctx) {
//    TS[0]-FIL[52]-SEL[2]-GBY[3]-RS[4]-GBY[5]-RS[42]-JOIN[48]-SEL[49]-LIM[50]-FS[51]
//      -FIL[53]-SEL[9]-GBY[10]-RS[11]-GBY[12]-RS[43]-JOIN[48]
//    ->
//
//
//    Map1: TS[0]
//    Map2:FIL[52]-SEL[2]-GBY[3]-RS[4]
//    Map3:FIL[53]-SEL[9]-GBY[10]-RS[11]
//    Reducer1:GBY[5]-RS[42]-JOIN[48]-SEL[49]-LIM[50]-FS[51]
//    Reducer2:GBY[12]-RS[43]
    tb1.setConf(new TableScanDesc(null));
    FilterOperator fil1 = new FilterOperator(ctx);
    fil1.setConf(new FilterDesc());
    SelectOperator sel2 = new SelectOperator(ctx);
    sel2.setConf(new SelectDesc());
    GroupByOperator gby3 = new GroupByOperator(ctx);
    gby3.setConf(new GroupByDesc());
    ReduceSinkOperator rs4 = new ReduceSinkOperator(ctx);
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(new Properties());
    rs4.setConf(new ReduceSinkDesc());
    rs4.getConf().setKeySerializeInfo(tableDesc);
    tb1.getChildOperators().add(fil1);
    fil1.getParentOperators().add(tb1);
    fil1.getChildOperators().add(sel2);
    sel2.getParentOperators().add(fil1);
    sel2.getChildOperators().add(gby3);
    gby3.getParentOperators().add(sel2);
    gby3.getChildOperators().add(rs4);
    rs4.getParentOperators().add(gby3);

  }

}
