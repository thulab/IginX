package cn.edu.tsinghua.iginx;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

public class Test {

  public static void main(String[] args) throws LpSolveException {
    System.out.println(System.getProperty("java.library.path"));
    LpSolve problem = LpSolve.makeLp(0, 5);
    problem.setMinim();
    problem.setObjFn(new double[]{0.0, -2.0, -1.0, -4.0, -3.0, -1.0});
    problem.addConstraint(new double[]{0, 0, 2.0, 1.0, 4.0, 2.0}, LpSolve.LE, 54);
    problem.addConstraint(new double[]{0, 3, 4, 5, -1, -1}, LpSolve.LE, 62);
    problem.setLowbo(3, 3.32);
    problem.setLowbo(4, 0.678);
    problem.setLowbo(5, 2.57);
    problem.setVerbose(LpSolve.IMPORTANT);
    problem.printLp();
    int ret = problem.solve();//具体返回值信息请参考lpsolve文档

    if (ret == LpSolve.OPTIMAL/*LpSolve.INFEASIBLE*/) {
      ;
    }
    //System.out.println(problem.getStatustext(ret));
    //System.out.println(problem.getObjective());
    //double[] vars = new double[6];
    //problem.getVariables(vars);
    //for(int i = 0; i < 6; i++) {
    //	System.out.println(vars[i]);
    //}
    //problem.printSolution(1);
    //problem.printConstraints(1);
    //problem.printObjective();
    //problem.writeLp("c:\\mytest.lp");

    //double[] newRow = { 0, 5, 6, 7, 8 };	// element 0 is unused
    //problem.setRow(1, newRow); //替换原来的 constraint
    //problem.setAddRowmode(true);//加速构建
    //problem.setPresolve() //待研究
    //problem.setRhVec(new double[] {0.0, 6.0, 7.5});//设置右手边向量，从数组 [1]开始算
    //problem.setInt(1, true);//设置成整数变量
    //problem.setBinary(1,true)//设置成binary型，只能取0或1
    //problem.getSolutioncount();//具有相同最优目标值的解的个数
    //problem.setSolutionLimit(int)//设置取哪个解
    //problem.getSolutionLimit()//得到上面设置的值
    //get_lp_index //和presolve相关的东西，暂时不研究
    problem.deleteLp();
  }
}
