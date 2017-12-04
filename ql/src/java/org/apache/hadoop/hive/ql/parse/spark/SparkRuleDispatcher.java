/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.spark;

import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public class SparkRuleDispatcher implements Dispatcher  {
  private final Multimap<Rule, NodeProcessor> procRules;
  private final NodeProcessorCtx procCtx;
  private final Set<NodeProcessor> defaultProcSet;

  /**
   * Constructor.
   *
   * @param defaultProc
   *          default processor to be fired if no rule matches
   * @param rules
   *          operator processor that handles actual processing of the node
   * @param procCtx
   *          operator processor context, which is opaque to the dispatcher
   */
  public SparkRuleDispatcher(NodeProcessor defaultProc,
                             Multimap<Rule, NodeProcessor> rules, NodeProcessorCtx procCtx) {
    this.defaultProcSet = new HashSet<NodeProcessor>();
    this.defaultProcSet.add(defaultProc);
    procRules = rules;
    this.procCtx = procCtx;
  }
  /**
   * Dispatcher function.
   *
   * @param nd
   *          operator to process
   * @param ndStack
   *          the operators encountered so far
   * @throws SemanticException
   */
  @Override
  public Object dispatch(Node nd, Stack<Node> ndStack, Object... nodeOutputs)
      throws SemanticException {

    // find the firing rule
    // find the rule from the stack specified
    Rule rule = null;
    int minCost = Integer.MAX_VALUE;
    for (Rule r : procRules.keySet()) {
      int cost = r.cost(ndStack);
      if ((cost >= 0) && (cost < minCost)) {
        minCost = cost;
        rule = r;
      }
    }

    Collection<NodeProcessor> procSet;

    if (rule == null) {
      procSet = defaultProcSet;
    } else {
      procSet = procRules.get(rule);
    }

    // Do nothing in case proc is null
    Object ret = null;
    for (NodeProcessor proc : procSet) {
      if (proc != null) {
        // Call the process function
        ret = proc.process(nd, ndStack, procCtx, nodeOutputs);
      }
    }
    return ret;

  }
}
