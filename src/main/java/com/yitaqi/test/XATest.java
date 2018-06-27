package com.yitaqi.test;

import com.mysql.jdbc.jdbc2.optional.MysqlXAConnection;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 描述:
 * ${DESCRIPTION}
 *
 * @author xue
 * @create 2018-06-25 17:07
 */
public class XATest {

    public static void main(String[] args) throws SQLException {

        // 获取资源管理器 RM1
        Connection connection =
                DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "000000");
        // true : 打印XA语句，用于调试
        XAConnection xAConnection = new MysqlXAConnection((com.mysql.jdbc.Connection) connection, true);
        XAResource rm1 = xAConnection.getXAResource();
        // 获取资源管理器 RM2
        Connection connection2 =
                DriverManager.getConnection("jdbc:mysql://localhost:3306/user", "root", "000000");
        XAConnection xaConnection2 = new MysqlXAConnection((com.mysql.jdbc.Connection) connection2, true);
        XAResource rm2 = xaConnection2.getXAResource();
        // AP请求执行一个分布式事物，TM 生成全局事物id
        byte[] gtrid = "contextId".getBytes();
        int formatId = 1;
        try {
            // 分别执行RM1 和RM2 上的事物分支
            // TM 生成 rm1 上的事物分支id
            byte[] branchId1 = "rm0001".getBytes();
            Xid xid1 = new MysqlXid(gtrid, branchId1, formatId);
            // 执行rm1 上的事物分支
            rm1.start(xid1, XAResource.TMNOFLAGS);
            PreparedStatement preparedStatement =
                    connection.prepareStatement("INSERT INTO USER (NAME) VALUES ('tom');");
            preparedStatement.execute();
            rm1.end(xid1, XAResource.TMSUCCESS);
            // TM 生成 rm2 上的事物分支id
            byte[] branchId2 = "rm0002".getBytes();
            Xid xid2 = new MysqlXid(gtrid, branchId2, formatId);
            // 执行rm2 上的事物分支
            rm2.start(xid2, XAResource.TMNOFLAGS);
            PreparedStatement preparedStatement2
                    = connection2.prepareStatement("INSERT INTO t_user (NAME) VALUES ('jim');");
            preparedStatement2.execute();
            rm2.end(xid2, XAResource.TMSUCCESS);
            // 两阶段提交
            // 询问所有的RM 准备提交事务分支
            int prepare1 = rm1.prepare(xid1);
            int prepare2 = rm2.prepare(xid2);
            // 提交所有事务分支
            // 所有事物分支都prepare 成功，提交所有事务，否则，所有事务回滚
            if (prepare1 == XAResource.XA_OK && prepare2 == XAResource.XA_OK) {
                // false : 不使用一阶段提交
                rm1.commit(xid1, false);
                rm2.commit(xid2, false);
            } else {
                rm1.rollback(xid1);
                rm2.rollback(xid2);
            }
        } catch (XAException e) {
            e.printStackTrace();
        }
    }
}
