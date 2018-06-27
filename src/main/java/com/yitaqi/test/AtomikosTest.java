package com.yitaqi.test;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;

import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import java.sql.*;
import java.util.Properties;

/**
 * 描述:
 * ${DESCRIPTION}
 *
 * @author xue
 * @create 2018-06-26 10:49
 */
public class AtomikosTest {

    public static void main(String[] args) {

        AtomikosDataSourceBean dataSource1 = createDataSource("test");
        AtomikosDataSourceBean dataSource2 = createDataSource("user");
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement pstm1 = null;
        PreparedStatement pstm2 = null;
        UserTransaction userTransaction = new UserTransactionImp();
        try {
            // 开启事务
            userTransaction.begin();
            // 执行 db1 上的sql
            con1 = dataSource1.getConnection();
            pstm1 = con1.prepareStatement("INSERT INTO USER (NAME) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            pstm1.setString(1, "soldier");
            pstm1.executeUpdate();
            ResultSet generatedKeys = pstm1.getGeneratedKeys();
            int userId = -1;
            while (generatedKeys.next()) {
                userId = generatedKeys.getInt(1);
            }

            // exception
            int i = 1/0;

            // 执行 db2 上的sql
            con2 = dataSource2.getConnection();
            pstm2 = con2.prepareStatement("INSERT INTO t_user(name, age) VALUES (?, ?)");
            pstm2.setString(1, "lucy");
            pstm2.setInt(2, 13);
            pstm2.executeUpdate();

            // 两阶段提交
            userTransaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                userTransaction.rollback();
            } catch (SystemException e1) {
                e1.printStackTrace();
            }
        } finally {
            if (pstm1 != null) {
                try {
                    pstm1.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstm2 != null) {
                try {
                    pstm2.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (con1 != null) {
                try {
                    con1.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (con2 != null) {
                try {
                    con2.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (dataSource1 != null) {
                dataSource1.close();
            }
            if (dataSource2 != null) {
                dataSource2.close();
            }
        }
    }

    private static AtomikosDataSourceBean createDataSource(String dbName) {

        // 连接池基本属性
        Properties properties = new Properties();
        properties.setProperty("url", "jdbc:mysql://localhost:3306/" + dbName);
        properties.setProperty("user", "root");
        properties.setProperty("password", "000000");

        // 使用AtomikosBean 封装 MysqlXADataSource
        AtomikosDataSourceBean atomikosDataSourceBean = new AtomikosDataSourceBean();
        atomikosDataSourceBean.setUniqueResourceName(dbName);
        atomikosDataSourceBean.setXaDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlXADataSource");
        atomikosDataSourceBean.setXaProperties(properties);
        return atomikosDataSourceBean;
    }
}
