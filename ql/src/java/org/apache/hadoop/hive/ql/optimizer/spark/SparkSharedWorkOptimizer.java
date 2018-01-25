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

package org.apache.hadoop.hive.ql.optimizer.spark;

import com.google.common.collect.Lists;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.SharedResult;
import org.apache.hadoop.hive.ql.optimizer.SharedTable;
import org.apache.hadoop.hive.ql.optimizer.SharedWorkOptimizer;
import org.apache.hadoop.hive.ql.optimizer.SharedWorkOptimizerCache;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
/**
 * Shared computation optimizer.
 *
 * <p>Originally, this rule would find scan operators over the same table
 * in the query plan and merge them if they met some preconditions.
 *
 *  TS   TS             TS
 *  |    |     ->      /  \
 *  Op   Op           Op  Op
 *
 * <p>A limitation in the current implementation is that the optimizer does not
 * go beyond a work boundary.
 *
 * <p>The optimization works with the Spark execution engine.
 **/

public class SparkSharedWorkOptimizer  extends Transform {

  private final static Logger LOG = LoggerFactory.getLogger(SparkSharedWorkOptimizer.class);

  private HashMap<TableScanOperator,TableScanOperator> replaceFilterTSMap = new HashMap();
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    final Map<String, TableScanOperator> topOps = pctx.getTopOps();
    if (topOps.size() < 2) {
      // Nothing to do, bail out
      return pctx;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Before SharedWorkOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
    }
    // Cache to use during optimization
    SharedWorkOptimizerCache optimizerCache = new SharedWorkOptimizerCache();

    // Gather information about the DPP table scans and store it in the cache
    gatherDPPTableScanOps(pctx, optimizerCache);

    // Map of dbName.TblName -> TSOperator
    Multimap<String, TableScanOperator> tableNameToOps = SharedWorkOptimizer.splitTableScanOpsByTable(pctx);

    // We enforce a certain order when we do the reutilization.
    // In particular, we use size of table x number of reads to
    // rank the tables.
    List<Map.Entry<String, Long>> sortedTables = SharedWorkOptimizer.rankTablesByAccumulatedSize(pctx);
    LOG.debug("Sorted tables by size: {}", sortedTables);

    // Execute optimization
    Multimap<String, TableScanOperator> existingOps = ArrayListMultimap.create();
    Set<Operator<?>> removedOps = new HashSet<>();
    for (Map.Entry<String, Long> tablePair : sortedTables) {
      String tableName = tablePair.getKey();
      for (TableScanOperator discardableTsOp : tableNameToOps.get(tableName)) {
        if (removedOps.contains(discardableTsOp)) {
          LOG.debug("Skip {} as it has been already removed", discardableTsOp);
          continue;
        }
        Collection<TableScanOperator> prevTsOps = existingOps.get(tableName);
        for (TableScanOperator retainableTsOp : prevTsOps) {
          if (removedOps.contains(retainableTsOp)) {
            LOG.debug("Skip {} as it has been already removed", retainableTsOp);
            continue;
          }

          // First we quickly check if the two table scan operators can actually be merged
          boolean mergeable = SharedWorkOptimizer.areMergeable(pctx, optimizerCache, retainableTsOp, discardableTsOp);
          if (!mergeable) {
            // Skip
            LOG.debug("{} and {} cannot be merged", retainableTsOp, discardableTsOp);
            continue;
          }

          // Secondly, we extract information about the part of the tree that can be merged
          // as well as some structural information (memory consumption) that needs to be
          // used to determined whether the merge can happen
          SharedResult sr = SharedWorkOptimizer.extractSharedOptimizationInfoForRoot(
            pctx, optimizerCache, retainableTsOp, discardableTsOp);

          // It seems these two operators can be merged.
          // Check that plan meets some preconditions before doing it.
          // In particular, in the presence of map joins in the upstream plan:
          // - we cannot exceed the noconditional task size, and
          // - if we already merged the big table, we cannot merge the broadcast
          // tables.
//          if (!SharedWorkOptimizer.validPreConditions(pctx, optimizerCache, sr)) {
//            // Skip
//            LOG.debug("{} and {} do not meet preconditions", retainableTsOp, discardableTsOp);
//            continue;
//          }

          // We can merge
          if (sr.retainableOps.size() > 1) {
            //For hive on tez, this feature may will merge two branches if more than 1 operator(TS) are same in the two
            //branches. but for hive on spark, currently this is no need( not support).

//            // More than TS operator
//            Operator<?> lastRetainableOp = sr.retainableOps.get(sr.retainableOps.size() - 1);
//            Operator<?> lastDiscardableOp = sr.discardableOps.get(sr.discardableOps.size() - 1);
//            if (lastDiscardableOp.getNumChild() != 0) {
//              List<Operator<? extends OperatorDesc>> allChildren =
//                  Lists.newArrayList(lastDiscardableOp.getChildOperators());
//              for (Operator<? extends OperatorDesc> op : allChildren) {
//                lastDiscardableOp.getChildOperators().remove(op);
//                op.replaceParent(lastDiscardableOp, lastRetainableOp);
//                lastRetainableOp.getChildOperators().add(op);
//              }
//            }

//            LOG.debug("Merging subtree starting at {} into subtree starting at {}", discardableTsOp, retainableTsOp);
          } else {
            // Only TS operator
            ExprNodeGenericFuncDesc exprNode = null;
            if (retainableTsOp.getConf().getFilterExpr() != null) {
              // Push filter on top of children
              SharedWorkOptimizer.pushFilterToTopOfTableScan(optimizerCache, retainableTsOp);
              // Clone to push to table scan
              exprNode = (ExprNodeGenericFuncDesc) retainableTsOp.getConf().getFilterExpr();
            }
            if (discardableTsOp.getConf().getFilterExpr() != null) {
              // Push filter on top
              SharedWorkOptimizer.pushFilterToTopOfTableScan(optimizerCache, discardableTsOp);
              ExprNodeGenericFuncDesc tsExprNode = discardableTsOp.getConf().getFilterExpr();
              if (exprNode != null && !exprNode.isSame(tsExprNode)) {
                // We merge filters from previous scan by ORing with filters from current scan
                if (exprNode.getGenericUDF() instanceof GenericUDFOPOr) {
                  List<ExprNodeDesc> newChildren = new ArrayList<>(exprNode.getChildren().size() + 1);
                  for (ExprNodeDesc childExprNode : exprNode.getChildren()) {
                    if (childExprNode.isSame(tsExprNode)) {
                      // We do not need to do anything, it is in the OR expression
                      break;
                    }
                    newChildren.add(childExprNode);
                  }
                  if (exprNode.getChildren().size() == newChildren.size()) {
                    newChildren.add(tsExprNode);
                    exprNode = ExprNodeGenericFuncDesc.newInstance(
                        new GenericUDFOPOr(),
                        newChildren);
                  }
                } else {
                  exprNode = ExprNodeGenericFuncDesc.newInstance(
                      new GenericUDFOPOr(),
                      Arrays.<ExprNodeDesc>asList(exprNode, tsExprNode));
                }
              }
            }
            // Replace filter
            //TODO: understand why filters are not combined when there is more than 1 operator(TS) are same
            //in the two branch
            retainableTsOp.getConf().setFilterExpr(exprNode);
            replaceFilterTSMap.put(discardableTsOp, retainableTsOp);
            LOG.debug("Merging {} into {}", discardableTsOp, retainableTsOp);
          }

          LOG.info("SharedTable.getInstance().addSharedTable< "+discardableTsOp.getOperatorId()+ " ,"+retainableTsOp.getOperatorId()+" >");
          SharedTable.getInstance().addSharedTable(discardableTsOp,retainableTsOp);
          // First we remove the input operators of the expression that
          // we are going to eliminate
          for (Operator<?> op : sr.discardableInputOps) {
            //TODO Verify we need optimizerCache.removeOp(op)
            optimizerCache.removeOp(op);
            removedOps.add(op);
            // Remove DPP predicates
            if (op instanceof ReduceSinkOperator) {
              SemiJoinBranchInfo sjbi = pctx.getRsToSemiJoinBranchInfo().get(op);
              if (sjbi != null && !sr.discardableOps.contains(sjbi.getTsOp()) &&
                  !sr.discardableInputOps.contains(sjbi.getTsOp())) {
                //TODO To find similar code in Spark
//                GenTezUtils.removeSemiJoinOperator(
//                    pctx, (ReduceSinkOperator) op, sjbi.getTsOp());
              }
            } else if (op instanceof AppMasterEventOperator) {
              DynamicPruningEventDesc dped = (DynamicPruningEventDesc) op.getConf();
              if (!sr.discardableOps.contains(dped.getTableScan()) &&
                  !sr.discardableInputOps.contains(dped.getTableScan())) {
                //TODO To find similar code in Spark
//                GenTezUtils.removeSemiJoinOperator(
//                    pctx, (AppMasterEventOperator) op, dped.getTableScan());
              }
            }
            LOG.debug("Input operator removed: {}", op);
          }

          removedOps.add(discardableTsOp);
          // Finally we remove the expression from the tree
          for (Operator<?> op : sr.discardableOps) {
         //TODO Verify we need optimizerCache.removeOp(op)
            optimizerCache.removeOp(op);
            removedOps.add(op);
            if (sr.discardableOps.size() == 1) {
              // If there is a single discardable operator, it is a TableScanOperator
              // and it means that we have merged filter expressions for it. Thus, we
              // might need to remove DPP predicates from the retainable TableScanOperator
              Collection<Operator<?>> c =
                  optimizerCache.tableScanToDPPSource.get((TableScanOperator) op);
              for (Operator<?> dppSource : c) {
                if (dppSource instanceof ReduceSinkOperator) {
                  //TODO To find similar code in Spark
//                  GenTezUtils.removeSemiJoinOperator(pctx,
//                      (ReduceSinkOperator) dppSource,
//                      (TableScanOperator) sr.retainableOps.get(0));
                } else if (dppSource instanceof AppMasterEventOperator) {
                  //TODO To find similar code in Spark
//                  GenTezUtils.removeSemiJoinOperator(pctx,
//                      (AppMasterEventOperator) dppSource,
//                      (TableScanOperator) sr.retainableOps.get(0));
                }
              }
            }
            LOG.debug("Operator removed: {}", op);
          }

          break;
        }

        if (removedOps.contains(discardableTsOp)) {
          // This operator has been removed, remove it from the list of existing operators
          existingOps.remove(tableName, discardableTsOp);
        } else {
          // This operator has not been removed, include it in the list of existing operators
          existingOps.put(tableName, discardableTsOp);
        }
      }
    }

    // Remove unused table scan operators
    Iterator<Map.Entry<String, TableScanOperator>> it = topOps.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, TableScanOperator> e = it.next();
      if (e.getValue().getNumChild() == 0) {
        it.remove();
      }
    }

    //combine all conditions of TS to 1 condition
    for(TableScanOperator discardableTsOp:replaceFilterTSMap.keySet()){
      TableScanOperator retainedTsOp = replaceFilterTSMap.get(discardableTsOp);
      if( retainedTsOp.getConf().getFilterExpr()!= null) {
        discardableTsOp.getConf().setFilterExpr(
            retainedTsOp.getConf().getFilterExpr());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("After SharedWorkOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
    }

    return pctx;
  }

  private static void gatherDPPTableScanOps(
    ParseContext pctx, SharedWorkOptimizerCache optimizerCache) throws SemanticException {
    // Find TS operators with partition pruning enabled in plan
    // because these TS may potentially read different data for
    // different pipeline.
    // These can be:
    // 1) TS with DPP.
    // 2) TS with semijoin DPP.
    Map<String, TableScanOperator> topOps = pctx.getTopOps();
    Collection<Operator<? extends OperatorDesc>> tableScanOps =
      Lists.<Operator<?>>newArrayList(topOps.values());
    Set<SparkPartitionPruningSinkOperator> s =
      OperatorUtils.findOperators(tableScanOps, SparkPartitionPruningSinkOperator.class);
    for (SparkPartitionPruningSinkOperator a : s) {
      if (a.getConf() instanceof SparkPartitionPruningSinkDesc) {
        SparkPartitionPruningSinkDesc dped = (SparkPartitionPruningSinkDesc) a.getConf();
        optimizerCache.tableScanToDPPSource.put(dped.getTableScan(), a);
      }
    }
    for (Map.Entry<ReduceSinkOperator, SemiJoinBranchInfo> e
      : pctx.getRsToSemiJoinBranchInfo().entrySet()) {
      optimizerCache.tableScanToDPPSource.put(e.getValue().getTsOp(), e.getKey());
    }
    LOG.debug("DPP information stored in the cache: {}", optimizerCache.tableScanToDPPSource);
  }
}
