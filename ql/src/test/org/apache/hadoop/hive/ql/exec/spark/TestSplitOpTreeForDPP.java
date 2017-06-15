package org.apache.hadoop.hive.ql.exec.spark;



import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * Created by root on 6/15/17.
 */
public class TestSplitOpTreeForDPP {

  //FIL[2]-SEL[3]-SPARKPARTIINPRUNING[10]
  //       -SEL[4] -SPARKPARTITIONPRUNING[11]
  //       -RS[5]
  @Test
  public void test_getJoinOperator() {
    final Node fil2 = new Node("FIL[2]", OperatorType.Filter);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node sel4 = new Node("SEL[4]", OperatorType.SEL);
    final Node rs5 = new Node("RS[5]", OperatorType.ReduceSink);
    final Node spr10 = new Node("SPARKPARTITIONPRUNINGSINK[10]",OperatorType.SPARKPRUNINGSINK);
    final Node spr11 = new Node("SPARKPARTITIONPRUNINGSINK[11]",OperatorType.SPARKPRUNINGSINK);
    fil2.setChildOperators(new ArrayList() {
      {
        add(sel3);
        add(sel4);
        add(rs5);
      }
    });
    
    sel3.setParentOperators(new ArrayList() {
      {
        add(fil2);
      }
    });
    
    sel3.setChildOperators(new ArrayList() {
      {
        add(spr10);
      }
    });

    sel4.setParentOperators(new ArrayList() {
      {
        add(fil2);
      }
    });

    sel4.setChildOperators(new ArrayList() {
      {
        add(spr11);
      }
    });
    
    rs5.setParentOperators(new ArrayList() {
      {
        add(fil2);
      }
    });
    
    
    spr10.setParentOperators(new ArrayList() {
      {
        add(sel3);
      }
    });
    
    spr11.setParentOperators(new ArrayList() {
      {
        add(sel4);
      }
    });

    List<Node> firstNodeOfPruningBranch = findFirstNodesOfPruningBranch(fil2);
    Assert.assertEquals(firstNodeOfPruningBranch.size(), 2);
    
  }

  private List<Node> findFirstNodesOfPruningBranch(Node filterOp) {
    Stack<Node> queue = new Stack();
    List<Node> firstNodeOfBranches =new ArrayList<>();
    queue.add(filterOp);
    ArrayList<Node> visitedList = new ArrayList<>();
    while( !queue.isEmpty()){
      Node p = queue.peek();
      Node unvisitedChild = getUnvisitedChild(p,visitedList);
      if( unvisitedChild!=null){
        if( unvisitedChild.operatorType== OperatorType.SPARKPRUNINGSINK){
          // the second element is the first node of branch which contains
          // SparkPartitionPruningOperator
          firstNodeOfBranches.add(queue.get(1));
        }else{
          queue.add(unvisitedChild);
        }
        visitedList.add(unvisitedChild);
      }else{
        //go back to the previous node
        queue.pop();
      }
    }
    return firstNodeOfBranches;
  }

  private Node getUnvisitedChild(Node p, ArrayList<Node> visitedList) {
    Node res = null;
    List<Node> children = p.getChildOperators();
    if( children!= null && children.size()>0){
      for(Node child: children){
        if(!visitedList.contains(child)){
          res = child;
          break;
        }
      }
    }
    return res;
  }

}
