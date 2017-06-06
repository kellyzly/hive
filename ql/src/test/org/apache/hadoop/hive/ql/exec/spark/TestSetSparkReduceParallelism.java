package org.apache.hadoop.hive.ql.exec.spark;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by lzhang66 on 5/26/2017.
 */
public class TestSetSparkReduceParallelism {

  //TS[0]-SEL[1]-RS[2]-SEL[3]-RS[5]-FOR[6]-GBY[7]-LIMIT[8]-FS[10]
  //                                       -GBY[11]-SEL[12]-FS[14]
  @Test
  public void test_getJoinOperator() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node rs5 = new Node("RS[5]", OperatorType.ReduceSink);
    final Node for6 = new Node("FOR[6]", OperatorType.FOR);
    final Node gby7 = new Node("GBY[7]", OperatorType.GBY);
    final Node limit8 = new Node("LIMIT[8]", OperatorType.Limit);
    final Node fs10 = new Node("FS[10]", OperatorType.FileSink);
    final Node gby11 = new Node("GBY[11]", OperatorType.GBY);
    final Node sel12 = new Node("SEL[12]", OperatorType.SEL);
    final Node fs14 = new Node("FS[14]", OperatorType.FileSink);
    rs2.setChildOperators(new ArrayList() {
      {
        add(sel3);
      }
    });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList() {{
      add(rs5);
    }});

    rs5.setParentOperators(new ArrayList() {{
      add(sel3);
    }});
    rs5.setChildOperators(new ArrayList() {
      {
        add(for6);
      }
    });
    for6.setParentOperators(new ArrayList() {{
      add(rs5);
    }});
    for6.setChildOperators(new ArrayList() {
      {
        add(gby7);
        add(gby11);
      }
    });
    gby7.setParentOperators(new ArrayList() {{
      add(for6);
    }});
    gby7.setChildOperators(new ArrayList() {
      {
        add(limit8);
      }
    });
    limit8.setParentOperators(new ArrayList() {{
      add(gby7);
    }});
    limit8.setChildOperators(new ArrayList() {
      {
        add(fs10);
      }
    });

    fs10.setParentOperators(new ArrayList() {{
      add(limit8);
    }});

    gby11.setParentOperators(new ArrayList() {{
      add(for6);
    }});
    gby11.setChildOperators(new ArrayList() {
      {
        add(sel12);
      }
    });

    sel12.setParentOperators(new ArrayList() {{
      add(gby11);
    }});
    sel12.setChildOperators(new ArrayList() {
      {
        add(fs14);
      }
    });

    fs14.setParentOperators(new ArrayList() {{
      add(sel12);
    }});

    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertTrue(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertFalse(isOrderByLimit);

  }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-LIMIT[4]-RS[5]-FOR[6]-GBY[7]-SEL[8]-FS[10]
  //                                               -GBY[11]-SEL[12]-FS[14]
  @Test
  public void test_getJoinOperator2() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node limit4 = new Node("LIMIT[4]", OperatorType.Limit);
    final Node rs5 = new Node("RS[5]", OperatorType.ReduceSink);
    final Node for6 = new Node("FOR[6]", OperatorType.FOR);
    final Node gby7 = new Node("GBY[7]", OperatorType.GBY);
    final Node sel8 = new Node("SEL[8]", OperatorType.SEL);
    final Node fs10 = new Node("FS[10]", OperatorType.FileSink);
    final Node gby11 = new Node("GBY[11]", OperatorType.GBY);
    final Node sel12 = new Node("SEL[12]", OperatorType.SEL);
    final Node fs14 = new Node("FS[14]", OperatorType.FileSink);
    rs2.setChildOperators(new ArrayList() {
      {
        add(sel3);
      }
    });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList() {{
      add(limit4);
    }});

    limit4.setParentOperators(new ArrayList<Node>() {
                                {
                                  add(sel3);
                                }
                              }
    );
    limit4.setChildOperators(new ArrayList<Node>() {{
                               add(rs5);
                             }}
    );

    rs5.setParentOperators(new ArrayList() {{
      add(limit4);
    }});
    rs5.setChildOperators(new ArrayList() {
      {
        add(for6);
      }
    });
    for6.setParentOperators(new ArrayList() {{
      add(rs5);
    }});
    for6.setChildOperators(new ArrayList() {
      {
        add(gby7);
        add(gby11);
      }
    });
    gby7.setParentOperators(new ArrayList() {{
      add(for6);
    }});
    gby7.setChildOperators(new ArrayList() {
      {
        add(sel8);
      }
    });
    sel8.setParentOperators(new ArrayList() {{
      add(gby7);
    }});
    sel8.setChildOperators(new ArrayList() {
      {
        add(fs10);
      }
    });

    fs10.setParentOperators(new ArrayList() {{
      add(sel8);
    }});

    gby11.setParentOperators(new ArrayList() {{
      add(for6);
    }});
    gby11.setChildOperators(new ArrayList() {
      {
        add(sel12);
      }
    });

    sel12.setParentOperators(new ArrayList() {{
      add(gby11);
    }});
    sel12.setChildOperators(new ArrayList() {
      {
        add(fs14);
      }
    });

    fs14.setParentOperators(new ArrayList() {{
      add(sel12);
    }});

    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertTrue(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertTrue(isOrderByLimit);

  }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-SEL[4]-RS[5]-FOR[6]-GBY[7]-LIMIT[8]-FS[10]
  //                                             -GBY[11]-SEL[12]-FS[14]
  //                                 LIMIT[15]-SPARKPRUNESINK[16]
  @Test
  public void test_getJoinOperator5() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node sel4 = new Node("SEL[4]", OperatorType.SEL);
    final Node rs5 = new Node("RS[5]", OperatorType.ReduceSink);
    final Node for6 = new Node("FOR[6]", OperatorType.FOR);
    final Node gby7 = new Node("GBY[7]", OperatorType.GBY);
    final Node limit8 = new Node("Limit[8]", OperatorType.Limit);
    final Node fs10 = new Node("FS[10]", OperatorType.FileSink);
    final Node gby11 = new Node("GBY[11]", OperatorType.GBY);
    final Node sel12 = new Node("SEL[12]", OperatorType.SEL);
    final Node fs14 = new Node("FS[14]", OperatorType.FileSink);
    final Node limit15 = new Node("LIMIT[15]", OperatorType.Limit);
    final Node sparkpruneSink16 = new Node("SPARKPRUNESINK[16]", OperatorType.SPARKPRUNINGSINK);
    rs2.setChildOperators(new ArrayList() {
      {
        add(sel3);
      }
    });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList() {{
      add(sel4);
    }});

    sel4.setParentOperators(new ArrayList<Node>() {
                              {
                                add(sel3);
                              }
                            }
    );
    sel4.setChildOperators(new ArrayList<Node>() {{
                             add(rs5);
                             add(limit15);
                           }}
    );

    rs5.setParentOperators(new ArrayList() {{
      add(sel4);
    }});
    rs5.setChildOperators(new ArrayList() {
      {
        add(for6);

      }
    });
    for6.setParentOperators(new ArrayList() {{
      add(rs5);
    }});
    for6.setChildOperators(new ArrayList() {
      {
        add(gby7);
        add(gby11);
      }
    });
    gby7.setParentOperators(new ArrayList() {{
      add(for6);
    }});
    gby7.setChildOperators(new ArrayList() {
      {
        add(limit8);
      }
    });
    limit8.setParentOperators(new ArrayList() {{
      add(gby7);
    }});
    limit8.setChildOperators(new ArrayList() {
      {
        add(fs10);
      }
    });

    fs10.setParentOperators(new ArrayList() {{
      add(limit8);
    }});

    gby11.setParentOperators(new ArrayList() {{
      add(for6);
    }});
    gby11.setChildOperators(new ArrayList() {
      {
        add(sel12);
      }
    });

    sel12.setParentOperators(new ArrayList() {{
      add(gby11);
    }});
    sel12.setChildOperators(new ArrayList() {
      {
        add(fs14);
      }
    });

    fs14.setParentOperators(new ArrayList() {{
      add(sel12);
    }});

    limit15.setParentOperators(new ArrayList() {{
      add(sel4);
    }});

    limit15.setChildOperators(new ArrayList() {{
      add(sparkpruneSink16);
    }});


    sparkpruneSink16.setParentOperators(new ArrayList() {{
      add(limit15);
    }});
    Node jointOperator = getJoinOperator(rs2);
    System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is " + jointOperator.getOperatorId());
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertTrue(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertTrue(isOrderByLimit);

  }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-LIMIT[4]-RS[5]
  @Test
  public void test_getJoinOperator3() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node limit4 = new Node("LIMIT[4]", OperatorType.Limit);
    final Node rs5 = new Node("RS[5]", OperatorType.ReduceSink);
    rs2.setChildOperators(new ArrayList() {
      {
        add(sel3);
      }
    });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList() {{
      add(limit4);
    }});

    limit4.setParentOperators(new ArrayList<Node>() {
                                {
                                  add(sel3);
                                }
                              }
    );
    limit4.setChildOperators(new ArrayList<Node>() {{
                               add(rs5);
                             }}
    );

    rs5.setParentOperators(new ArrayList() {{
      add(limit4);
    }});

    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertFalse(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertTrue(isOrderByLimit);

  }

  //TS[0]-SEL[1]-RS[2]-SEL[3]-RS[5]-LIMIT[6]
  @Test
  public void test_getJoinOperator4() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node rs5 = new Node("RS[5]", OperatorType.ReduceSink);
    final Node limit6 = new Node("LIMIT[6]", OperatorType.Limit);
    rs2.setChildOperators(new ArrayList() {
      {
        add(sel3);
      }
    });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList() {{
      add(rs5);
    }});
    rs5.setParentOperators(new ArrayList() {{
      add(sel3);
    }});
    rs5.setChildOperators(new ArrayList() {{
      add(limit6);
    }});


    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    System.out.println("IsMultiInsert :" + isMultiInsert);
    Assert.assertFalse(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertFalse(isOrderByLimit);
  }


  //TS[0]-SEL[1]-RS[2]-FS[5]
  //                   -FS[6]
  @Test
  public void test_getJoinOperator7() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node fs5 = new Node("FS[5]", OperatorType.FileSink);
    final Node fs6 = new Node("FS[6]", OperatorType.FileSink);

    rs2.setChildOperators(new ArrayList() {
      {
        add(fs5);
        add(fs6);
      }
    });

    fs5.setParentOperators(new ArrayList() {
      {
        add(rs2);
      }
    });

    fs6.setParentOperators(new ArrayList() {
      {
        add(rs2);
      }
    });
    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertTrue(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertFalse(isOrderByLimit);

  }


  //TS[0]-SEL[1]-RS[2]-FS[5]
  //                   -SPARKPRUNESINK[6]
  @Test
  public void test_getJoinOperator9() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node fs5 = new Node("FS[5]", OperatorType.FileSink);
    final Node sps = new Node("SPARKPRUNESINK[6]", OperatorType.SPARKPRUNINGSINK);

    rs2.setChildOperators(new ArrayList() {
      {
        add(fs5);
        add(sps);
      }
    });

    fs5.setParentOperators(new ArrayList() {
      {
        add(rs2);
      }
    });

    sps.setParentOperators(new ArrayList() {
      {
        add(rs2);
      }
    });

    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertFalse(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertFalse(isOrderByLimit);

  }

  //TS[0]-SEL[1]-RS[2]-FS[5]
  //                  -LIMIT[7]-SPARKPRUNESINK[6]
  @Test
  public void test_getJoinOperator10() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node fs5 = new Node("FS[5]", OperatorType.FileSink);
    final Node limit7 = new Node("LIMIT[7]", OperatorType.Limit);
    final Node sps = new Node("SPARKPRUNESINK[6]", OperatorType.SPARKPRUNINGSINK);

    rs2.setChildOperators(new ArrayList() {
      {
        add(fs5);
        add(limit7);
      }
    });
    fs5.setParentOperators(new ArrayList() {
      {
        add(rs2);
      }
    });

    limit7.setParentOperators(new ArrayList() {
      {
        add(rs2);
      }
    });

    limit7.setChildOperators(new ArrayList() {
      {
        add(sps);
      }
    });

    sps.setParentOperators(new ArrayList() {
      {
        add(limit7);
      }
    });

    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertFalse(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertTrue(isOrderByLimit);

  }


  //  TS[0]-RS[2]-SEL[3]-LIMIT[7]-FS[11]
//                             -FS[12]
  @Test
  public void test_getJoinOperator8() {
    final Node rs2 = new Node("RS[2]", OperatorType.ReduceSink);
    final Node sel3 = new Node("SEL[3]", OperatorType.SEL);
    final Node limit7 = new Node("SEL[7]", OperatorType.SEL);
    final Node fs11 = new Node("FS[11]", OperatorType.FileSink);
    final Node fs12 = new Node("FS[12]", OperatorType.FileSink);

    rs2.setChildOperators(new ArrayList() {{
      add(sel3);
    }});
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList() {{
      add(limit7);
    }});
    limit7.setParentOperators(new ArrayList() {{
      add(sel3);
    }});

    limit7.setChildOperators(new ArrayList() {{
      add(fs11);
      add(fs12);
    }});
    fs11.setParentOperators(new ArrayList() {{
      add(limit7);
    }});
    fs12.setParentOperators(new ArrayList() {{
      add(limit7);
    }});


    Node jointOperator = getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator != null ? true : false;
    Assert.assertFalse(isMultiInsert);
    boolean isOrderByLimit = isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertFalse(isOrderByLimit);
  }

  /**
   * Judge orderByLimit case:
   * non multi-insert: If there is a Limit in the path which is from reduceSink to next RS/FS, return true otherwise false
   * multi-insert: If there is a Limit in the path which is from reduceSink to joinOperator, return true otherwise false
   *
   * @param reduceSink
   * @param isMultiInsert
   * @param jointOperator
   */
  private boolean isOrderByLimit(Node reduceSink, boolean isMultiInsert, Node jointOperator) {
    boolean isOrderByLimit = false;
    List<Node> visited = new ArrayList<>();
    Stack<Node> stack = new Stack<>();
    stack.push(reduceSink);
    while (!stack.isEmpty()) {
      Node top = stack.peek();
      Node unvisitedChild = getUnvisitedChild(top, visited);
      if (unvisitedChild == null) {
        if (top.getChildOperators() == null || top.getChildOperators().size()==0) {
          //top is a leave node, generate a path from RS to FS/NONFS
          boolean tmpResult = HasOrderByLimitInPath(stack, isMultiInsert, jointOperator);
          if (tmpResult == true) {
            isOrderByLimit = true;
            break;
          }
        }
        //go back along the path to parent
        stack.pop();
      } else {
        stack.push(unvisitedChild);
        visited.add(unvisitedChild);
      }
    }

    return isOrderByLimit;
  }

  private boolean HasOrderByLimitInPath(Stack<Node> stack, boolean isMultiInsert, Node jointOperator) {
    boolean res = false;
    LinkedList queue = new LinkedList<>();
    System.out.println("Path:");
    for (int i = 0; i < stack.size(); i++) {
      System.out.println(stack.get(i).getOperatorId() + " ");
      queue.add(stack.get(i));
    }
    Node lastEle = stack.get(stack.size() - 1);
    if (isMultiInsert == false || (isMultiInsert == true && lastEle.operatorType != OperatorType.FileSink)) {
      res = HasOrderByLimitInNonMultiInsertPath(queue);
      System.out.println("hasOrderByLimitInNonMultiInsertPath:" + res);

    }
    if (isMultiInsert == true && lastEle.operatorType == OperatorType.FileSink) {
      res = HasOrderByLimitInMultiInsertPath(queue, jointOperator);
      System.out.println("hasOrderByLimitInMultiInsertPath:" + res);

    }
    return res;
  }

  private boolean HasOrderByLimitInMultiInsertPath(LinkedList<Node> queue, Node jointOperator) {
    boolean isOrderByLimit = false;
    for (Node operator : queue) {
      if (operator.getOperatorId().equals(jointOperator.getOperatorId())) {
        if ( operator.operatorType== OperatorType.Limit) {
          isOrderByLimit = true;
        }
        break;
      }

      if (operator.operatorType == OperatorType.Limit) {
        isOrderByLimit = true;
        break;
      }
    }
    return isOrderByLimit;
  }

  private boolean HasOrderByLimitInNonMultiInsertPath(Deque<Node> queue) {
    boolean isOrderByLimit = false;
    Node rootRS = queue.getFirst();
    for (Node operator : queue) {
      if ((operator.operatorType == OperatorType.ReduceSink && !operator.getOperatorId().equals(rootRS.getOperatorId())) || operator.operatorType == OperatorType.FileSink) {
        break;
      }

      if (operator.operatorType == OperatorType.Limit) {
        isOrderByLimit = true;
        break;
      }
    }
    return isOrderByLimit;
  }

  private Node getUnvisitedChild(Node parent, List visited) {
    Node res = null;
    if (parent.getChildOperators() != null) {
      for (int i = 0; i < parent.getChildOperators().size(); i++) {
        Node child = parent.getChildOperators().get(i);
        if (!visited.contains(child)) {
          res = child;
          break;
        }
      }
    }
    return res;
  }


  private Node getJoinOperator(Node rs) {
    int pathToFSNum = 0;
    Node jointOperator = null;
    Stack<Node> childQueue = new Stack<>();
    if (rs.getChildOperators() != null) {
      childQueue.addAll(rs.getChildOperators());
    }
    List<Node> commonPath = new ArrayList<>();
    while (!childQueue.isEmpty()) {
      if (pathToFSNum > 1) {
        // this is a multi insert case, we need not traverse the operator tree anymore
        break;
      }
      Node child = childQueue.pop();
      if (pathToFSNum < 1) {
        commonPath.add(child);
      } else if (!commonPath.contains(child) && pathToFSNum == 1) {
        if (jointOperator == null) {
          //this is a multi-insertCase, there are more than 1 paths from rs to the last FS
          //the parent of child is the JointOperator
          List<Node> parents = child.getParentOperators();
          if (parents.size() > 1) {
            System.out.println("Current operator is " + child + ", this is a multi insert case and there is only 1 parent of the child, but now the size of parents is " + parents.size());
          } else {
            jointOperator = parents.get(0);
          }
        }
      }
      if (child.operatorType == OperatorType.FileSink) {
        pathToFSNum = pathToFSNum + 1;
      } else {
        if (child.getChildOperators() != null) {
          childQueue.addAll(child.getChildOperators());
        }
      }
    }
    boolean isMultiInsert = pathToFSNum > 1 ? true : false;
    System.out.println("reducesink:" + rs + " isMultiInsert:" + isMultiInsert);
    return jointOperator;
  }
}
