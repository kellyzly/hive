/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.sun.org.apache.bcel.internal.generic.Select;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkProcContext;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkWork;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for GenSparkWork.
 *
 */
public class TestGenSparkWork {

  GenSparkProcContext ctx;
  GenSparkWork proc;

  TableScanOperator ts;
  FilterOperator fil52;
  SelectOperator sel2;
  GroupByOperator gby3;
  ReduceSinkOperator rs4;
  GroupByOperator gby5;
  MapJoinOperator mapJoin58;
  SelectOperator sel49;
  LimitOperator limit50;
  FileSinkOperator fs51;

  TableScanOperator ts2;
  FilterOperator fil53;
  SelectOperator sel9;
  GroupByOperator gby10;
  ReduceSinkOperator rs11;
  GroupByOperator gby12;
  ReduceSinkOperator rs43;



  /**
   * @throws java.lang.Exception
   */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    // Init conf
    final HiveConf conf = new HiveConf(SemanticAnalyzer.class);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SPARK_SHARED_WORK_OPTIMIZATION,true);
    SessionState.start(conf);

    // Init parse context
    final ParseContext pctx = new ParseContext();
    pctx.setContext(new Context(conf));

    Map<String, TableScanOperator> topOps = new HashMap();
    topOps.put("TS[0]",ts);
    topOps.put("TS[2]",ts2);
    ctx = new GenSparkProcContext(
        conf,
        pctx,
        Collections.EMPTY_LIST,
        new ArrayList<Task<? extends Serializable>>(),
        Collections.EMPTY_SET,
        Collections.EMPTY_SET,
        topOps);

    proc = new GenSparkWork(new GenSparkUtils() {
      @Override
      protected void setupMapWork(MapWork mapWork, GenSparkProcContext context,
                                  PrunedPartitionList partitions, TableScanOperator root, String alias)
          throws SemanticException {

        LinkedHashMap<String, Operator<? extends OperatorDesc>> map
            = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
        map.put("foo", root);
        mapWork.setAliasToWork(map);
        return;
      }
    });

    //  TS[0]-FIL[52]-SEL[2]-GBY[3]-RS[4]-GBY[5]-JOIN[58]-SEL[49]-LIM[50]-FS[51]
   //   TS[2]-FIL[53]-SEL[9]-GBY[10]-RS[11]-GBY[12]-RS[43]-JOIN[58]
    CompilationOpContext cCtx = new CompilationOpContext();

    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(new Properties());
    ts = new TableScanOperator(cCtx);

    fil52 = new FilterOperator(cCtx);
    sel2 = new SelectOperator(cCtx);
    gby3 = new GroupByOperator(cCtx);
    rs4 = new ReduceSinkOperator(cCtx);
    gby5 = new GroupByOperator(cCtx);
    mapJoin58 = new MapJoinOperator(cCtx);
    sel49 = new SelectOperator(cCtx);
    limit50 = new LimitOperator(cCtx);
    fs51 = new FileSinkOperator(cCtx);


    fil53 = new FilterOperator(cCtx);
    sel9 = new SelectOperator(cCtx);
    gby10 = new GroupByOperator(cCtx);
    rs11 = new ReduceSinkOperator(cCtx);
    gby12 = new GroupByOperator(cCtx);
    rs43 = new ReduceSinkOperator(cCtx);

    ts.setConf(new TableScanDesc(null));
    fil52.setConf(new FilterDesc());
    sel2.setConf(new SelectDesc());
    gby3.setConf(new GroupByDesc());
    rs4.setConf(new ReduceSinkDesc());
    rs4.getConf().setKeySerializeInfo(tableDesc);
    gby5.setConf(new GroupByDesc());
    mapJoin58.setConf(new MapJoinDesc());
    sel49.setConf(new SelectDesc());
    limit50.setConf(new LimitDesc());
    fs51.setConf(new FileSinkDesc());
    fs51.getConf().setTableInfo(tableDesc);


    fil53.setConf(new FilterDesc());
    sel9.setConf(new SelectDesc());
    gby10.setConf(new GroupByDesc());
    rs11.setConf(new ReduceSinkDesc());
    rs11.getConf().setKeySerializeInfo(tableDesc);
    gby12.setConf(new GroupByDesc());
    rs43.setConf(new ReduceSinkDesc());
    rs43.getConf().setKeySerializeInfo(tableDesc);


    ts.getChildOperators().add(fil52);

    fil52.getParentOperators().add(ts);
    fil52.getChildOperators().add(sel2);
    sel2.getParentOperators().add(fil52);
    sel2.getChildOperators().add(gby3);
    gby3.getParentOperators().add(sel2);
    gby3.getChildOperators().add(rs4);
    rs4.getParentOperators().add(gby3);
    rs4.getChildOperators().add(gby5);
    gby5.getParentOperators().add(rs4);
    gby5.getChildOperators().add(mapJoin58);
    mapJoin58.getParentOperators().add(gby5);
    mapJoin58.getChildOperators().add(sel49);
    sel49.getParentOperators().add(mapJoin58);
    sel49.getChildOperators().add(limit50);
    limit50.getParentOperators().add(sel49);
    limit50.getChildOperators().add(fs51);
    fs51.getParentOperators().add(limit50);



    ts2.getChildOperators().add(fil53);
    fil53.getParentOperators().add(ts2);
    fil53.getChildOperators().add(sel9);
    sel9.getParentOperators().add(fil53);
    sel9.getChildOperators().add(rs11);
    rs11.getParentOperators().add(sel9);
    rs11.getChildOperators().add(gby12);
    gby12.getParentOperators().add(rs11);
    gby12.getChildOperators().add(rs43);
    rs43.getParentOperators().add(gby12);
    rs43.getChildOperators().add(mapJoin58);

    ctx.preceedingWork = null;
    ctx.currentRootOperator = ts;
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    ctx = null;
    proc = null;
    ts = null;
    fil52 = null;
    sel2 = null;
    gby3= null;
    rs4 = null;
    gby5 = null;
    mapJoin58 = null;
    sel49 = null;
    limit50 = null;
    fs51 = null;


    ts2 = null;
    fil53 = null;
    sel9 = null;
    gby10 = null;
    rs11 = null;
    gby12 = null;
    rs43 = null;
  }


//  @Test
  //readd TestGenSparkWork2.
  //final HiveConf conf = new HiveConf(SemanticAnalyzer.class);
 // conf.setBoolVar(HiveConf.ConfVars.HIVE_SPARK_SHARED_WORK_OPTIMIZATION,false);
//  public void testCreateReduceWithoutSharedOpt() throws SemanticException {
//    // create map
//    proc.process(ts, null, ctx, (Object[])null);
//    proc.process(rs4,  null,  ctx,  (Object[])null);
//
//    // create reduce
//    proc.process(fs51, null, ctx, (Object[])null);
//
//    SparkWork work = ctx.currentTask.getWork();
//    assertEquals(work.getAllWork().size(),2);
//
//    BaseWork w = work.getAllWork().get(1);
//    assertTrue(w instanceof ReduceWork);
//    assertTrue(work.getParents(w).contains(work.getAllWork().get(0)));
//
//    ReduceWork rw = (ReduceWork)w;
//
//    // need to make sure names are set for tez to connect things right
//    assertNotNull(w.getName());
//
//    // map work should start with our ts op
//    assertSame(rw.getReducer(),fs51);
//
//    // should have severed the ties
//    assertEquals(fs51.getParentOperators().size(),0);
//  }


  @Test
  public void testCreateReduceWithSharedOpt() throws SemanticException {
    // create map
    proc.process(ts, null, ctx, (Object[])null);
    proc.process(rs4,  null,  ctx,  (Object[])null);
    proc.process(ts2,null,ctx, (Object[])null);
    proc.process(rs43, null, ctx, (Object[])null);

    // create reduce
    proc.process(fs51, null, ctx, (Object[])null);


    SparkWork work = ctx.currentTask.getWork();
    assertEquals(work.getAllWork().size(),3);
    //Map0
    BaseWork map0 = work.getAllWork().get(0);
    System.out.println(toString(map0.getAllRootOperators()));

   //Map 1
    BaseWork map1 = work.getAllWork().get(1);
    System.out.println(toString(map1.getAllRootOperators()));

    //Reducer 2
    BaseWork reducer1 = work.getAllWork().get(2);
    System.out.println(toString(reducer1.getAllRootOperators()));
  }

  public static String toString(Set<Operator<? extends OperatorDesc>> top) {
    StringBuilder builder = new StringBuilder();
    Set<String> visited = new HashSet<String>();
    for (Operator<?> op : top) {
      if (builder.length() > 0) {
        builder.append('\n');
      }
      toString(builder, visited, op, 0);
    }
    return builder.toString();
  }

  static boolean toString(StringBuilder builder, Set<String> visited, Operator<?> op, int start) {
    String name = op.toString();
    boolean added = visited.add(name);
    if (start > 0) {
      builder.append("-");
      start++;
    }
    builder.append(name);
    start += name.length();
    if (added) {
      if (op.getNumChild() > 0) {
        List<Operator<?>> children = op.getChildOperators();
        for (int i = 0; i < children.size(); i++) {
          if (i > 0) {
            builder.append('\n');
            for (int j = 0; j < start; j++) {
              builder.append(' ');
            }
          }
          toString(builder, visited, children.get(i), start);
        }
      }
      return true;
    }
    return false;
  }

}
