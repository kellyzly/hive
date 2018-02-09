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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.spark.DynamicValueRegistrySpark;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkRuntimeFilterPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.Callable;

public class SparkRuntimeFilterPruningSinkOperator extends Operator<SparkRuntimeFilterPruningSinkDesc> {
  @SuppressWarnings("deprecation")
  protected transient Serializer serializer;
  protected transient DataOutputBuffer buffer;
  protected static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeFilterPruningSinkOperator.class);
  protected Configuration hconf;
  protected Writable data;
  private List<Object> values = new ArrayList<>();
  protected ObjectCache dynamicValueCache;
  protected List<String> dynamicValueCacheKeys= new ArrayList<String>();

  /** Kryo ctor. */
  @VisibleForTesting
  public SparkRuntimeFilterPruningSinkOperator() {
    super();
  }

  public SparkRuntimeFilterPruningSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    serializer = (Serializer) ReflectionUtils.newInstance(
      conf.getTable().getDeserializerClass(), null);
    buffer = new DataOutputBuffer();
    this.hconf = hconf;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    ObjectInspector rowInspector = inputObjInspectors[0];
    try {
      Writable writableRow = serializer.serialize(row, rowInspector);
      flushToRegistrySpark(writableRow);
      //why write the follow code will throw: Failed to execute spark task, with exception 'java.lang.RuntimeException(Error caching reduce.xml)'
     // FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.spark.SparkTask. Error caching reduce.xml
      //values.add(writableRow);
      writableRow.write(buffer);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      try {
        flushToFile();
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  private void flushToRegistrySpark(Object value) throws Exception {
    // setup values registry
    String valueRegistryKey = DynamicValue.DYNAMIC_VALUE_REGISTRY_CACHE_KEY;
    String queryId = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVEQUERYID);

    dynamicValueCache = new ObjectCacheWrapper(
      new  org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);

    final DynamicValueRegistrySpark registrySpark = dynamicValueCache.retrieve(valueRegistryKey,
      new Callable<DynamicValueRegistrySpark>() {
        @Override
        public DynamicValueRegistrySpark call() {
          return new DynamicValueRegistrySpark();
        }
      });

    dynamicValueCacheKeys.add(valueRegistryKey);
    // copy DynamicValueRegistrySpark#init method

    RuntimeValuesInfo runtimeValuesInfo = this.getConf().getRuntimeValuesInfo();

    // Set up col expressions for the dynamic values using this input
    List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
      ois.add(expr.getWritableObjectInspector());
    }

    // Setup deserializer/obj inspectors for the incoming data source
    Deserializer deserializer = ReflectionUtils.newInstance(runtimeValuesInfo.getTableDesc().getDeserializerClass(), null);
    deserializer.initialize(hconf, runtimeValuesInfo.getTableDesc().getProperties());
    ObjectInspector inspector = deserializer.getObjectInspector();
    // Set up col expressions for the dynamic values using this input
    List<ExprNodeEvaluator> colExprEvaluators = new ArrayList<ExprNodeEvaluator>();
    for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
      ExprNodeEvaluator exprEval = ExprNodeEvaluatorFactory.get(expr, null);
      exprEval.initialize(inspector);
      colExprEvaluators.add(exprEval);
    }

    Object row = deserializer.deserialize((Writable) value);
    for (int colIdx = 0; colIdx < ois.size(); ++colIdx) {
      // Read each expression and save it to the value registry
      ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
      Object val = eval.evaluate(row);
      registrySpark.setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), val);
    }

    /*DynamicValueRegistrySpark.RegistryConfSpark registryConf = new DynamicValueRegistrySpark.RegistryConfSpark(jobConf,  result);
    registrySpark.init(registryConf);*/
    // here the key is refer the ObjectCacheWrapper#makeKey method
    org.apache.hadoop.hive.ql.exec.mr.ObjectCache.dynamicValueRegistryMap.put(queryId + "_" + valueRegistryKey, registrySpark);
  }

  private void flushToFile() throws IOException {
    // write an intermediate file to the specified path
    // the format of the path is: tmpPath/targetWorkId/sourceWorkId/randInt
    Path path = conf.getPath();
    FileSystem fs = path.getFileSystem(this.getConfiguration());
    fs.mkdirs(path);

    while (true) {
      path = new Path(path, String.valueOf(Utilities.randGen.nextInt()));
      if (!fs.exists(path)) {
        break;
      }
    }

    short numOfRepl = fs.getDefaultReplication(path);

    ObjectOutputStream out = null;
    FSDataOutputStream fsout = null;

    try {
      fsout = fs.create(path, numOfRepl);
      out = new ObjectOutputStream(new BufferedOutputStream(fsout, 4096));
      out.writeUTF(conf.getTargetColumnName());
      buffer.writeTo(out);
    } catch (Exception e) {
      try {
        fs.delete(path, false);
      } catch (Exception ex) {
        LOG.warn("Exception happened while trying to clean partial file.");
      }
      throw e;
    } finally {
      if (out != null) {
        LOG.info("Flushed to file: " + path);
        out.close();
      } else if (fsout != null) {
        fsout.close();
      }
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SPARKRUNTIMEFILTERPRUNINGSINK;
  }

  @Override
  public String getName() {
    return SparkRuntimeFilterPruningSinkOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return "SPARKRUNTIMEFILTERPRUNINGSINK";
  }
}
