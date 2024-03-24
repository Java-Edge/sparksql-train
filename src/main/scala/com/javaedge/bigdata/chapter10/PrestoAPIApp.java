package com.javaedge.bigdata.chapter10;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class PrestoAPIApp {

    public static void main(String[] args) throws Exception {

        Class.forName("com.facebook.presto.jdbc.PrestoDriver");

        Connection connection = DriverManager.getConnection("jdbc:presto://ruozedata001:8080/hive/test_db", "hadoop", "");
        Statement statement = connection.createStatement();
//        ResultSet resultSet = statement.executeQuery("show tables");
//        while (resultSet.next()) {
//            System.out.println(resultSet.getString(1));
//        }



//        ResultSet resultSet = statement.executeQuery("select * from emp");
//        while (resultSet.next()) {
//            System.out.println(resultSet.getInt("empno") + "\t" + resultSet.getString("ename"));
//        }


        ResultSet resultSet = statement.executeQuery("select e.empno,e.ename,e.deptno,d.dname from hive.test_db.emp e join mysql.test_db.dept d on e.deptno=d.deptno");
        while (resultSet.next()) {
            System.out.println(resultSet.getInt("empno") + "\t" + resultSet.getString("ename") + "\t" +
                    resultSet.getInt("deptno") + "\t" + resultSet.getString("dname"));
        }

    }
}
