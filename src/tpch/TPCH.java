/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tpch;

import connection.ConnectionFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import queries.RefreshFunction2;
import queries.TPCHQueriesTime;

/**
 *
 * @author LTorres
 */
public class TPCH {

    // mysql, monetdb, postgresql
    static String SGBD = "mysql";
    static String DB = "tpch1";
    static String user = "root";
    static String password = "mysql";

    public static void main(String[] args) throws IOException, SQLException {

        ConnectionFactory cf = new ConnectionFactory(SGBD, DB, user, password);
        Connection con = cf.getConnection();
        System.out.println("Connected to " + SGBD + " - " + DB);
        
        Power p = new Power(con, 1);
        p.run();
        p.saveTimes();
        
        //TPCHQueriesTime tpchQT = new TPCHQueriesTime();
        //tpchQT.getTpchq().setConnection(con);
        //RefreshFunction2 rf2 = new RefreshFunction2();
        //rf2.getIndexes("D:\\TPCH\\DATA\\ORIGINAL\\RF\\1GB\\delete.1");
        //tpchQT.run();
        //tpchQT.saveQueriesTime("queries_time.txt");
    }
}
