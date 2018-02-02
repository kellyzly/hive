package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.exec.Operator;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SharedResult {
  public final List<Operator<?>> retainableOps;
  public final List<Operator<?>> discardableOps;
  public final Set<Operator<?>> discardableInputOps;
  final long dataSize;
  final long maxDataSize;

  public SharedResult(Collection<Operator<?>> retainableOps, Collection<Operator<?>> discardableOps,
                       Set<Operator<?>> discardableInputOps, long dataSize, long maxDataSize) {
    this.retainableOps = ImmutableList.copyOf(retainableOps);
    this.discardableOps = ImmutableList.copyOf(discardableOps);
    this.discardableInputOps = ImmutableSet.copyOf(discardableInputOps);
    this.dataSize = dataSize;
    this.maxDataSize = maxDataSize;
  }

  @Override
  public String toString() {
    return "SharedResult { " + this.retainableOps + "; " + this.discardableOps + "; "
      + this.discardableInputOps + "};";
  }
}
