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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * the second M for M->M->R in Hive on Spark
 */
public class SparkMapOperator2 extends MapOperator {


//Must be implemented from SparkMapRecordHandler
// mo.initialize(jc, null);
// mo.setChildren(job);
//  mo.passExecContext(execContext);
//  mo.initializeLocalWork(jc);
//  mo.initializeMapOperator(jc);

  private Map<Operator<?>, MapOpCtx> opCtxMap =
      new HashMap<Operator<?>, MapOpCtx>();
  // child operator --> object inspector (converted OI if it's needed)
  private final Map<Operator<?>, StructObjectInspector> childrenOpToOI =
      new HashMap<Operator<?>, StructObjectInspector>();
  public SparkMapOperator2(CompilationOpContext ctx) {
    super(ctx);
  }
  @Override
  public void initEmptyInputChildren(List<Operator<?>> children, Configuration hconf) throws SerDeException, Exception {

  }


  public void setChildrenFromRoot(Operator root) throws Exception {
    //How to get the children ,before How does MapOeprator do?
    // org.apache.hadoop.hive.ql.plan.MapWork.getPathToAliases() to get

    //org.apache.hadoop.hive.ql.exec.MapOperator.opCtxMap's function: seems no big function just to initialize MapOperator#currentCtxs
    List<Operator<? extends OperatorDesc>> children = new ArrayList<>();
    children.add(root);
    initOperatorContext(children);

    // we found all the operators that we are supposed to process.
    setChildOperators(children);
  }

  private void initOperatorContext(List<Operator<? extends OperatorDesc>> children)
      throws HiveException {
    //TODO where is opCtxMap initilization
      for (MapOpCtx context : opCtxMap.values()) {
        if (!children.contains(context.op)) {
          continue;
        }
        StructObjectInspector prev =
            childrenOpToOI.put(context.op, context.rowObjectInspector);
        if (prev != null && !prev.equals(context.rowObjectInspector)) {
          throw new HiveException("Conflict on row inspector for " + context.alias);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("dump " + context.op + " " + context.rowObjectInspector.getTypeName());
        }
      }
  }


  @Override
  public Deserializer getCurrentDeserializer() {
    return null;
  }

  @Override
  public void process(Writable value) throws HiveException {

  }

  @Override
  public void process(Object row, int tag) throws HiveException {

  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public OperatorType getType() {
    return null;
  }
}
