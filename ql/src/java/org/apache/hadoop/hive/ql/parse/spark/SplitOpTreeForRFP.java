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
package org.apache.hadoop.hive.ql.parse.spark;


import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.*;

public class SplitOpTreeForRFP implements NodeProcessor{
  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
    SparkRuntimeFilterPruningSinkOperator pruningSinkOperator = (SparkRuntimeFilterPruningSinkOperator)nd;
    GenSparkProcContext context = (GenSparkProcContext)procCtx;

    // Locate the op where the branch starts
    // This is guaranteed to succeed since the branch always follow the pattern
    // as shown in the first picture above.
    Operator<?> parentSelOp = pruningSinkOperator;
    Operator<?> childSelOp = null;
    while(parentSelOp != null){
      if(parentSelOp.getNumChild() > 1) {
        break;
      } else {
        childSelOp = parentSelOp;
        parentSelOp = parentSelOp.getParentOperators().get(0);
      }
    }

    // Check if this is a MapJoin. If so, do not split.
    for (Operator<?> childOp : parentSelOp.getChildOperators()) {
      if (childOp instanceof ReduceSinkOperator &&
        childOp.getChildOperators().get(0) instanceof MapJoinOperator) {
        //TODO whether need to add
        //context.pruningSinkSet.add(pruningSinkOperator);
        return null;
      }
    }
    List<Operator<?>> roots = new LinkedList<Operator<?>>();
    collectRoots(roots, pruningSinkOperator);

    List<Operator<?>> savedChildOps = parentSelOp.getChildOperators();
    parentSelOp.setChildOperators(Utilities.makeList(childSelOp));

    //Now clone the tree above childSelOp
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots);
    for (int i = 0; i < roots.size(); i++) {
      TableScanOperator newTs = (TableScanOperator) newRoots.get(i);
      TableScanOperator oldTs = (TableScanOperator) roots.get(i);
      newTs.getConf().setTableMetadata(oldTs.getConf().getTableMetadata());
    }
    context.clonedRuntimeFilterPruningTableScanSet.addAll(newRoots);

    // Restore broken links between operators, and remove the branch from the original tree
    parentSelOp.setChildOperators(savedChildOps);
    parentSelOp.removeChild(childSelOp);

    // Find the cloned PruningSink and add it to pruningSinkSet
    Set<Operator<?>> sinkSet = new HashSet<Operator<?>>();
    for (Operator<?> root : newRoots) {
      SparkUtilities.collectOp(sinkSet, root, SparkRuntimeFilterPruningSinkOperator.class);
    }

    SparkRuntimeFilterPruningSinkOperator clonedPruningSinkOp =
      (SparkRuntimeFilterPruningSinkOperator) sinkSet.iterator().next();
    clonedPruningSinkOp.getConf().setTableScan(pruningSinkOperator.getConf().getTableScan());
    context.runtimeFilterPruningSinkSet.add(clonedPruningSinkOp);
    context.clonedPruningSinkRuntimeValuesInfo.put(clonedPruningSinkOp, context.parseContext.getRfToRuntimeValuesInfo().get(pruningSinkOperator));

    return null;
  }

  /**
   * Recursively collect all roots (e.g., table scans) that can be reached via this op.
   * @param result contains all roots can be reached via op
   * @param op the op to examine.
   */
  private void collectRoots(List<Operator<?>> result, Operator<?> op) {
    if (op.getNumParent() == 0) {
      result.add(op);
    } else {
      for (Operator<?> parentOp : op.getParentOperators()) {
        collectRoots(result, parentOp);
      }
    }
  }
}
