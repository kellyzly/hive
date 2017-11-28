package org.apache.hadoop.hive.ql.exec.spark;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import junit.framework.Assert;
import org.apache.hadoop.hive.ql.parse.spark.SparkRuleDispatcher;
import org.junit.Test;

/**
 * Created by lzhang66 on 5/26/2017.
 */
public class TestGenSpark {




//  TS[0]-FIL[52]-SEL[2]-GBY[3]-RS[4]-GBY[5]-MAPJOIN[58]-SEL[49]-LIM[50]-FS[51]
//      -FIL[53]-SEL[9]-GBY[10]-RS[11]-GBY[12]-RS[43]-MAPJOIN[58]

//      -FIL[54]-SEL[16]-GBY[17]-RS[18]-GBY[19]-RS[44]-MAPJOIN[58]
//      -FIL[55]-SEL[23]-GBY[24]-RS[25]-GBY[26]-RS[45]-MAPJOIN[58]
//      -FIL[56]-SEL[30]-GBY[31]-RS[32]-GBY[33]-RS[46]-MAPJOIN[58]
//      -FIL[57]-SEL[37]-GBY[38]-RS[39]-GBY[40]-RS[47]-MAPJOIN[58]
  @Test
  public void test_getJoinOperator() {
    final Node ts0 = new Node("TS[0]",OperatorType.TableScan);
    final Node fil52 = new Node("FIL[52]",OperatorType.Filter);
    final Node sel2 = new Node("SEL[2]", OperatorType.SEL);
    final Node gby3 = new Node("GBY[3]",OperatorType.GBY);
    final Node rs4 = new Node("RS[4]", OperatorType.ReduceSink);
    final Node gby5 = new Node("GBY[5]",OperatorType.GBY);
    final Node mapjoin58 = new Node("MAPJOIN[58]", OperatorType.MAPJOIN);
    final Node sel49 = new Node("SEL[49]", OperatorType.SEL);
    final Node limit50 = new Node("LIMIT[50]", OperatorType.Limit);
    final Node fs51 = new Node("FS[51]", OperatorType.FileSink);
    final Node fil53 = new Node("FIL[53]",OperatorType.Filter);
    final Node sel19 = new Node("SEL[9]", OperatorType.SEL);
    final Node gby10 = new Node("GBY[10]",OperatorType.GBY);
    final Node rs11 = new Node("RS[11]", OperatorType.ReduceSink);
    final Node gby12 = new Node("GBY[12]",OperatorType.GBY);
    final Node rs43 = new Node("RS[43]", OperatorType.ReduceSink);


    ts0.setChildOperators(new ArrayList() {
      {
        add(fil52);
        add(fil53);
      }
    });
    fil52.setParentOperators(new ArrayList() {{
      add(sel2);
    }});
    sel2.setChildOperators(new ArrayList() {{
      add(gby3);
    }});

    gby3.setParentOperators(new ArrayList() {{
      add(rs4);
    }});
    rs4.setChildOperators(new ArrayList() {
      {
        add(gby5);
      }
    });
    gby5.setParentOperators(new ArrayList() {{
      add(mapjoin58);
    }});
    mapjoin58.setChildOperators(new ArrayList() {
      {
        add(sel49);
      }
    });
    sel49.setParentOperators(new ArrayList() {{
      add(limit50);
    }});
    limit50.setChildOperators(new ArrayList() {
      {
        add(fs51);
      }
    });
    fil53.setParentOperators(new ArrayList() {{
      add(sel19);
    }});
    sel19.setChildOperators(new ArrayList() {
      {
        add(gby10);
      }
    });

    gby10.setParentOperators(new ArrayList() {{
      add(rs11);
    }});

    rs11.setParentOperators(new ArrayList() {{
      add(gby12);
    }});
    gby12.setChildOperators(new ArrayList() {
      {
        add(rs43);
      }
    });

    rs43.setParentOperators(new ArrayList() {{
      add(mapjoin58);
    }});

//    SparkRuleDispatcher d
//    Assert.assertTrue(isMultiInsert);


  }





}