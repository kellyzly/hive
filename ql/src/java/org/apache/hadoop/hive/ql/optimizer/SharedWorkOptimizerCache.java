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
package org.apache.hadoop.hive.ql.optimizer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;

import java.util.List;
import java.util.Set;

/**
 * Cache to accelerate optimization
 */
public class SharedWorkOptimizerCache {
  // Operators that belong to each work
  final HashMultimap<Operator<?>, Operator<?>> operatorToWorkOperators =
    HashMultimap.<Operator<?>, Operator<?>>create();
  // Table scan operators to DPP sources
  public final Multimap<TableScanOperator, Operator<?>> tableScanToDPPSource =
    HashMultimap.<TableScanOperator, Operator<?>>create();

  // Add new operator to cache work group of existing operator (if group exists)
  void putIfWorkExists(Operator<?> opToAdd, Operator<?> existingOp) {
    List<Operator<?>> c = ImmutableList.copyOf(operatorToWorkOperators.get(existingOp));
    if (!c.isEmpty()) {
      for (Operator<?> op : c) {
        operatorToWorkOperators.get(op).add(opToAdd);
      }
      operatorToWorkOperators.putAll(opToAdd, c);
      operatorToWorkOperators.put(opToAdd, opToAdd);
    }
  }

  // Remove operator
  public void removeOp(Operator<?> opToRemove) {
    Set<Operator<?>> s = operatorToWorkOperators.get(opToRemove);
    s.remove(opToRemove);
    List<Operator<?>> c1 = ImmutableList.copyOf(s);
    if (!c1.isEmpty()) {
      for (Operator<?> op1 : c1) {
        operatorToWorkOperators.remove(op1, opToRemove); // Remove operator
      }
      operatorToWorkOperators.removeAll(opToRemove); // Remove entry for operator
    }
  }

  // Remove operator and combine
  public void removeOpAndCombineWork(Operator<?> opToRemove, Operator<?> replacementOp) {
    Set<Operator<?>> s = operatorToWorkOperators.get(opToRemove);
    s.remove(opToRemove);
    List<Operator<?>> c1 = ImmutableList.copyOf(s);
    List<Operator<?>> c2 = ImmutableList.copyOf(operatorToWorkOperators.get(replacementOp));
    if (!c1.isEmpty() && !c2.isEmpty()) {
      for (Operator<?> op1 : c1) {
        operatorToWorkOperators.remove(op1, opToRemove); // Remove operator
        operatorToWorkOperators.putAll(op1, c2); // Add ops of new collection
      }
      operatorToWorkOperators.removeAll(opToRemove); // Remove entry for operator
      for (Operator<?> op2 : c2) {
        operatorToWorkOperators.putAll(op2, c1); // Add ops to existing collection
      }
    }
  }

  @Override
  public String toString() {
    return "SharedWorkOptimizerCache { \n" + operatorToWorkOperators.toString() + "\n };";
  }
}

