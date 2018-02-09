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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.Callable;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.ObjectCacheWrapper;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.DynamicValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;


public class SparkRuntimeFilterPruner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDynamicPartitionPruner.class);
  private final BytesWritable writable = new BytesWritable();
  protected Map<String, Object> result = Collections.synchronizedMap(new HashMap<String, Object>());
  protected ObjectCache dynamicValueCache;
  //protected List<String> dynamicValueCacheKeys= new ArrayList<String>();

  public void prune(MapWork work, JobConf jobConf) throws HiveException, SerDeException {
    Set<String> sourceWorkIds = work.getRfSourceTableDescMap().keySet();
    if (sourceWorkIds.size() == 0) {
      return;
    }
    ObjectInputStream in = null;
    try {
      Path baseDir = work.getTmpPathForRuntimeFilter();
      FileSystem fs = FileSystem.get(baseDir.toUri(), jobConf);
      for (String id : sourceWorkIds) {
        Path sourceDir = new Path(baseDir, id);
        //here the fileStatus in only one path
        for (FileStatus fileStatus : fs.listStatus(sourceDir)) {
          // here the table only one, because the one table -> one TS-> one SparkRuntimeFilterOperator
          List<TableDesc> tables = work.getRfSourceTableDescMap().get(id);
          Deserializer deserializer = ReflectionUtils.newInstance(tables.get(0).getDeserializerClass(), null);
          deserializer.initialize(jobConf, tables.get(0).getProperties());

          in = new ObjectInputStream(fs.open(fileStatus.getPath()));

          in.readUTF();
          // Read fields
          while (in.available() > 0) {
            writable.readFields(in);
            Object row = deserializer.deserialize(writable);
            result.put("Reducer " + id, row);
          }
        }
      }
      flushToRegistrySpark(jobConf, work);
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        throw new HiveException("error while trying to close input stream", e);
      }
    }
  }

  private void flushToRegistrySpark(JobConf jobConf, MapWork work) throws Exception {
    // setup values registry
    String valueRegistryKey = DynamicValue.DYNAMIC_VALUE_REGISTRY_CACHE_KEY;
    String queryId = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVEQUERYID);

   /* dynamicValueCache = new ObjectCacheWrapper(
      new  org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);*/
    dynamicValueCache = ObjectCacheFactory.getCache(jobConf, queryId, false);

    final DynamicValueRegistrySpark registrySpark = dynamicValueCache.retrieve(valueRegistryKey,
      new Callable<DynamicValueRegistrySpark>() {
        @Override
        public DynamicValueRegistrySpark call() {
          return new DynamicValueRegistrySpark();
        }
      });

    //dynamicValueCacheKeys.add(valueRegistryKey);
    DynamicValueRegistrySpark.RegistryConfSpark registryConf = new DynamicValueRegistrySpark.RegistryConfSpark(jobConf, work, result);
    registrySpark.init(registryConf);
    // here the key is refer the ObjectCacheWrapper#makeKey method
    org.apache.hadoop.hive.ql.exec.mr.ObjectCache.dynamicValueRegistryMap.put(queryId + "_" + valueRegistryKey, registrySpark);
  }
}
