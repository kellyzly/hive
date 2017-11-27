
package org.apache.hadoop.hive.ql.exec.spark;

import java.util.List;

public class Node{
      private String operatorId;
      public OperatorType operatorType;
      private List<Node> childOperators;
      private List<Node> parentOperators;

      public List<Node> getChildOperators() {
          return childOperators;
      }

      public List<Node> getParentOperators() {
          return parentOperators;
      }

      public String getOperatorId() {
          return operatorId;
      }

    public void setChildOperators(List<Node> childOperators) {
        this.childOperators = childOperators;
    }

    public void setParentOperators(List<Node> parentOperators) {
        this.parentOperators = parentOperators;
    }

    Node(String operatorId, OperatorType operatorType){
          this.operatorId = operatorId;
        this.operatorType = operatorType;
      }
  }
    enum OperatorType{
      TableScan,
      Filter,
        ReduceSink,
        FileSink,
        SEL,
        FOR,
        GBY,
        Limit,
      MAPJOIN,
      SPARKPRUNINGSINK;
    }
