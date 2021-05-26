package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.query.expression.BooleanExpression;
import cn.edu.tsinghua.iginx.query.expression.TreeNode;
import junit.framework.TestCase;

public class BooleanExpressionTest extends TestCase {

    BooleanExpression booleanExpression;

    public void setUp() throws Exception {
        super.setUp();
        booleanExpression = new BooleanExpression("!(!a.b.a>3&&(!a.b.b!=3 or a.b.c=\"123\" and not a.b.d<=0.7 or !(a.b.e<>1.15)))");
    }

    public void testGetRoot() {
        TreeNode root = booleanExpression.getRoot();
        assertEquals(root.getData().getOperator().isReversed(), false);
    }

    public void testSetPostfixExpression() {
        String postfixExpression = booleanExpression.asString(booleanExpression.getPostfixExpression());
        assertEquals(" a.b.a  3  >  a.b.b  3  !=  a.b.c  \"123\"  !=  a.b.d  0.7  <=  ||  &&  a.b.e  1.15  !=  &&  || "
                , postfixExpression);
    }
}