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

    //TS[0]-SEL[1]-RS[2]-SEL[3]-RS[5]-FOR[6]-GBY[7]-SEL[8]-FS[10]
   //                                       -GBY[11]-SEL[12]-FS[14]
   @Test
    public void test_getJoinOperator(){
       final Node rs2 = new Node("RS[2]",OperatorType.ReduceSink);
     final Node  sel3 = new Node("SEL[3]",OperatorType.SEL);
     final  Node rs5 = new Node("RS[5]",OperatorType.ReduceSink);
     final Node for6= new Node("FOR[6]",OperatorType.FOR);
     final  Node gby7 = new Node("GBY[7]",OperatorType.GBY);
     final Node sel8 = new Node("SEL[8]",OperatorType.SEL);
     final   Node fs10 = new Node("FS[10]",OperatorType.FileSink);
     final   Node gby11= new Node("GBY[11]",OperatorType.GBY);
     final  Node sel12 = new Node("SEL[12]",OperatorType.SEL);
     final Node fs14 = new Node("FS[14]",OperatorType.FileSink);
        rs2.setChildOperators(new ArrayList(){
            {
                add(sel3);
            } });
        sel3.setParentOperators(new ArrayList() {{
            add(rs2);
        }});
        sel3.setChildOperators(new ArrayList(){{
              add(rs5);
        }});

        rs5.setParentOperators(new ArrayList(){{
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
                add(sel8);
            }
        });
        sel8.setParentOperators(new ArrayList(){{
            add(gby7);
        }});
        sel8.setChildOperators(new ArrayList(){{
        add(fs10);}
        });

        fs10.setParentOperators(new ArrayList(){{
            add(sel8);
        }});

        gby11.setParentOperators(new ArrayList(){{
            add(for6);
        }});
        gby11.setChildOperators(new ArrayList(){{
        add(sel12);}
        });

        sel12.setParentOperators(new ArrayList(){{
            add(gby11);
        }});
        sel12.setChildOperators(new ArrayList(){{
            add(fs14);}
        });

        fs14.setParentOperators(new ArrayList(){{
            add(sel12);
        }});

        Node jointOperator=   getJoinOperator(rs2);
        System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is "+jointOperator.getOperatorId() );
        boolean isMultiInsert = jointOperator!= null?true: false;
        boolean isOrderByLimit= isOrderByLimit(rs2, isMultiInsert, jointOperator);
        System.out.println("isOrderByLimit: "+isOrderByLimit);

    }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-LIMIT[4]-RS[5]-FOR[6]-GBY[7]-SEL[8]-FS[10]
  //                                       -GBY[11]-SEL[12]-FS[14]
  @Test
  public void test_getJoinOperator2(){
    final Node rs2 = new Node("RS[2]",OperatorType.ReduceSink);
    final Node  sel3 = new Node("SEL[3]",OperatorType.SEL);
    final Node limit4 = new Node("LIMIT[4]",OperatorType.Limit);
    final  Node rs5 = new Node("RS[5]",OperatorType.ReduceSink);
    final Node for6= new Node("FOR[6]",OperatorType.FOR);
    final  Node gby7 = new Node("GBY[7]",OperatorType.GBY);
    final Node sel8 = new Node("SEL[8]",OperatorType.SEL);
    final   Node fs10 = new Node("FS[10]",OperatorType.FileSink);
    final   Node gby11= new Node("GBY[11]",OperatorType.GBY);
    final  Node sel12 = new Node("SEL[12]",OperatorType.SEL);
    final Node fs14 = new Node("FS[14]",OperatorType.FileSink);
    rs2.setChildOperators(new ArrayList(){
      {
        add(sel3);
      } });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList(){{
      add(limit4);
    }});

    limit4.setParentOperators(new ArrayList<Node>()
    {{
        add(sel3);                        }
                              }
    );
    limit4.setChildOperators(new ArrayList<Node>()
        {{
            add(rs5);
        }}
    );

    rs5.setParentOperators(new ArrayList(){{
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
    sel8.setParentOperators(new ArrayList(){{
      add(gby7);
    }});
    sel8.setChildOperators(new ArrayList(){{
      add(fs10);}
    });

    fs10.setParentOperators(new ArrayList(){{
      add(sel8);
    }});

    gby11.setParentOperators(new ArrayList(){{
      add(for6);
    }});
    gby11.setChildOperators(new ArrayList(){{
      add(sel12);}
    });

    sel12.setParentOperators(new ArrayList(){{
      add(gby11);
    }});
    sel12.setChildOperators(new ArrayList(){{
      add(fs14);}
    });

    fs14.setParentOperators(new ArrayList(){{
      add(sel12);
    }});

    Node jointOperator=   getJoinOperator(rs2);
    System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is "+jointOperator.getOperatorId() );
    boolean isMultiInsert = jointOperator!= null?true: false;
    boolean isOrderByLimit= isOrderByLimit(rs2, isMultiInsert, jointOperator);
    System.out.println("isOrderByLimit: "+isOrderByLimit);

  }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-LIMIT[4]-RS[5]
  @Test
  public void test_getJoinOperator3(){
    final Node rs2 = new Node("RS[2]",OperatorType.ReduceSink);
    final Node  sel3 = new Node("SEL[3]",OperatorType.SEL);
    final Node limit4 = new Node("LIMIT[4]",OperatorType.Limit);
    final  Node rs5 = new Node("RS[5]",OperatorType.ReduceSink);
    rs2.setChildOperators(new ArrayList(){
      {
        add(sel3);
      } });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList(){{
      add(limit4);
    }});

    limit4.setParentOperators(new ArrayList<Node>()
                              {{
                                  add(sel3);                        }
                              }
    );
    limit4.setChildOperators(new ArrayList<Node>()
                             {{
                                 add(rs5);
                               }}
    );

    rs5.setParentOperators(new ArrayList(){{
      add(limit4);
    }});

    Node jointOperator=   getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator!= null?true: false;
    System.out.println("IsMultiInsert :" + isMultiInsert);
    if( isMultiInsert == true ) {
      System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is " + jointOperator.getOperatorId());
    }
    boolean isOrderByLimit= isOrderByLimit(rs2, isMultiInsert, jointOperator);
    System.out.println("isOrderByLimit: "+isOrderByLimit);

  }

  //TS[0]-SEL[1]-RS[2]-SEL[3]-RS[5]
  @Test
  public void test_getJoinOperator4(){
    final Node rs2 = new Node("RS[2]",OperatorType.ReduceSink);
    final Node  sel3 = new Node("SEL[3]",OperatorType.SEL);
    final  Node rs5 = new Node("RS[5]",OperatorType.ReduceSink);
    rs2.setChildOperators(new ArrayList(){
      {
        add(sel3);
      } });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList(){{
      add(rs5);
    }});


    Node jointOperator=   getJoinOperator(rs2);
    boolean isMultiInsert = jointOperator!= null?true: false;
    System.out.println("IsMultiInsert :"+isMultiInsert);
    if( isMultiInsert == true ) {
      System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is " + jointOperator.getOperatorId());
    }
    boolean isOrderByLimit= isOrderByLimit(rs2, isMultiInsert, jointOperator);
    System.out.println("isOrderByLimit: "+isOrderByLimit);
  }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-SEL[4]-RS[5]-FOR[6]-GBY[7]-LIMIT[8]-FS[10]
  //                                       -GBY[11]-SEL[12]-FS[14]
  @Test
  public void test_getJoinOperator5(){
    final Node rs2 = new Node("RS[2]",OperatorType.ReduceSink);
    final Node  sel3 = new Node("SEL[3]",OperatorType.SEL);
    final Node sel4 = new Node("SEL[4]",OperatorType.SEL);
    final  Node rs5 = new Node("RS[5]",OperatorType.ReduceSink);
    final Node for6= new Node("FOR[6]",OperatorType.FOR);
    final  Node gby7 = new Node("GBY[7]",OperatorType.GBY);
    final Node limit8 = new Node("Limit[8]",OperatorType.Limit);
    final   Node fs10 = new Node("FS[10]",OperatorType.FileSink);
    final   Node gby11= new Node("GBY[11]",OperatorType.GBY);
    final  Node sel12 = new Node("SEL[12]",OperatorType.SEL);
    final Node fs14 = new Node("FS[14]",OperatorType.FileSink);
    rs2.setChildOperators(new ArrayList(){
      {
        add(sel3);
      } });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList(){{
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
                             }}
    );

    rs5.setParentOperators(new ArrayList(){{
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

    fs10.setParentOperators(new ArrayList(){{
      add(limit8);
    }});

    gby11.setParentOperators(new ArrayList(){{
      add(for6);
    }});
    gby11.setChildOperators(new ArrayList(){{
      add(sel12);}
    });

    sel12.setParentOperators(new ArrayList(){{
      add(gby11);
    }});
    sel12.setChildOperators(new ArrayList(){{
      add(fs14);}
    });

    fs14.setParentOperators(new ArrayList(){{
      add(sel12);
    }});

    Node jointOperator=   getJoinOperator(rs2);
    System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is "+jointOperator.getOperatorId() );
    boolean isMultiInsert = jointOperator!= null?true: false;
    boolean isOrderByLimit= isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertFalse(isOrderByLimit);
    System.out.println("isOrderByLimit: "+isOrderByLimit);

  }


  //TS[0]-SEL[1]-RS[2]-SEL[3]-LIMIT[4]-RS[5]-
  //                                  -FS[14]
  @Test
  public void test_getJoinOperator7(){
    final Node rs2 = new Node("RS[2]",OperatorType.ReduceSink);
    final Node  sel3 = new Node("SEL[3]",OperatorType.SEL);
    final Node limit4 = new Node("LIMIT[4]",OperatorType.Limit);
    final  Node rs5 = new Node("RS[5]",OperatorType.ReduceSink);
    final Node fs6= new Node("FS[6]",OperatorType.FileSink);

    rs2.setChildOperators(new ArrayList(){
      {
        add(sel3);
      } });
    sel3.setParentOperators(new ArrayList() {{
      add(rs2);
    }});
    sel3.setChildOperators(new ArrayList(){{
      add(limit4);
    }});

    limit4.setParentOperators(new ArrayList<Node>()
                              {{
                                  add(sel3);                        }
                              }
    );
    limit4.setChildOperators(new ArrayList<Node>()
                             {{
                                 add(rs5);
                               }}
    );

    rs5.setParentOperators(new ArrayList(){{
      add(limit4);
    }});

    fs6.setParentOperators(new ArrayList() {{
      add(limit4);
    }});


    Node jointOperator=   getJoinOperator(rs2);
    System.out.println("jointOperator.getOperatorId() should be FOR[6], the actual value is "+jointOperator.getOperatorId() );
    boolean isMultiInsert = jointOperator!= null?true: false;
    boolean isOrderByLimit= isOrderByLimit(rs2, isMultiInsert, jointOperator);
    Assert.assertTrue(isOrderByLimit);
    System.out.println("isOrderByLimit: "+isOrderByLimit);

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
    Deque<Node> deque = new LinkedList<>();
    if (reduceSink.getChildOperators() != null) {
      deque.addAll(reduceSink.getChildOperators());
    }
    while (!deque.isEmpty()) {
      Node operator = deque.pop();
      if (!isMultiInsert && (operator.operatorType == OperatorType.ReduceSink || operator.operatorType == OperatorType.FileSink)) {
        break;
      }
      if (isMultiInsert && operator.getOperatorId().equals(jointOperator.getOperatorId())) {
        if( operator.operatorType == OperatorType.Limit){
          isOrderByLimit = true;
        }
        break;
      }
      if (operator.operatorType == OperatorType.Limit) {
        isOrderByLimit = true;
        break;
      } else {
        if (operator.getChildOperators() != null) {
          deque.addAll(operator.getChildOperators());
        }
      }
    }
    System.out.println("reduceSink:" + reduceSink + " isOrderByLimit:" + isOrderByLimit);
    return isOrderByLimit;
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
        //this is a multi-insertCase, there are more than 1 paths from rs to the last FS
        //the parent of child is the JointOperator
        if (jointOperator == null) {
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
