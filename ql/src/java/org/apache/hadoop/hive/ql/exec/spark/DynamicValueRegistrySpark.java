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

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.Map;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.DynamicValueRegistry;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.tez.DynamicValueRegistryTez;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.LogicalInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynamicValueRegistrySpark implements DynamicValueRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicValueRegistryTez.class);

  public static class RegistryConfSpark extends RegistryConf {
    public Configuration conf;
    public BaseWork baseWork;
    public Map<String, LogicalInput> inputs;
    protected Map<String, Object> result = Collections.synchronizedMap(new HashMap<String, Object>());

    public RegistryConfSpark(Configuration conf, BaseWork basework, Map<String, Object> result) {
      super();
      this.conf = conf;
      this.baseWork = basework;
      this.result = result;
    }
  }

  protected Map<String, Object> values = Collections.synchronizedMap(new HashMap<String, Object>());


  @Override
  public Object getValue(String key) {
    if (!values.containsKey(key)) {
      throw new IllegalStateException("Value does not exist in registry: " + key);
    }
    return values.get(key);
  }

  public void setValue(String key, Object value) {
    values.put(key, value);
  }

  @Override
  public void init(RegistryConf conf) throws Exception {
    DynamicValueRegistrySpark.RegistryConfSpark rct = (DynamicValueRegistrySpark.RegistryConfSpark) conf;

    for (String inputSourceName : rct.baseWork.getInputSourceToRuntimeValuesInfo().keySet()) {
      LOG.info("Runtime value source: " + inputSourceName);
      RuntimeValuesInfo runtimeValuesInfo = rct.baseWork.getInputSourceToRuntimeValuesInfo().get(inputSourceName);

      // Set up col expressions for the dynamic values using this input
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
        ois.add(expr.getWritableObjectInspector());
      }

      // Setup deserializer/obj inspectors for the incoming data source
      Deserializer deserializer = ReflectionUtils.newInstance(runtimeValuesInfo.getTableDesc().getDeserializerClass(), null);
      deserializer.initialize(rct.conf, runtimeValuesInfo.getTableDesc().getProperties());
      ObjectInspector inspector = deserializer.getObjectInspector();
      // Set up col expressions for the dynamic values using this input
      List<ExprNodeEvaluator> colExprEvaluators = new ArrayList<ExprNodeEvaluator>();
      for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
        ExprNodeEvaluator exprEval = ExprNodeEvaluatorFactory.get(expr, null);
        exprEval.initialize(inspector);
        colExprEvaluators.add(exprEval);
      }

      Object row = rct.result.get(inputSourceName);
      for (int colIdx = 0; colIdx < ois.size(); ++colIdx) {
        // Read each expression and save it to the value registry
        ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
        Object val = eval.evaluate(row);
        setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), val);
      }

    }
  }
}
