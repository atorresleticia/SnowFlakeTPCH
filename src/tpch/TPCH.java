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
import queries.TPCHQueries;
import queries.TPCHQueriesTime;

/**
 *
 * @author LTorres
 */
public class TPCH {

    private static String sgbd;
    private static String db;
    private static String user;
    private static String password;
    private static String scaleFactor;
    private static String port;

    private static void setSGBDParams(String banco) {
        if (banco.equals("postgresql")) {
            sgbd = "postgresql";
            user = "postgres";
            password = "wars218psql";
            port = "5432";
        }
        else if (banco.equals("monetdb")) {
            sgbd = "monetdb";
            user = "monetdb";
            password = "monetdb";
            port = "50000";
        }
    }

    private static void setSizeParams(String size) {
        switch (size) {
            case "1":
                scaleFactor = "1";
                db += "1";
                break;
            case "10":
                scaleFactor = "10";
                db += "10";
                break;
            case "30":
                scaleFactor = "30";
                db += "30";
                break;
        }
    }

    private static void power(TPCHCalc tpchCalc, Connection con) {

        System.out.println("Power running...");
        Power p = new Power(con, Integer.valueOf(scaleFactor), sgbd);
        p.run();

        tpchCalc.setSumQueryTotalTime(p.getQueryExecutionTime());
        tpchCalc.setSumRefreshTotalTime(p.getRefreshExecutionTime());
        System.out.println("Power Test Execution Time = " + (tpchCalc.getQueryTotalTime() + tpchCalc.getRefreshTotalTime()));
    }

    private static void throughput(TPCHCalc tpchCalc, Connection con) {

        System.out.println("Throughput running...");
        Throughput t = new Throughput(con, Integer.valueOf(scaleFactor), sgbd);
        t.run();

        System.out.println("Throughput Test Execution Time = " + t.getTime());
    }

    private static void queries(Connection con) {

        TPCHCalc tpchCalc = new TPCHCalc();

        System.out.println("Queries running...");
        TPCHQueriesTime q = new TPCHQueriesTime();
        q.getTpchq().setConnection(con);
        q.run();

        tpchCalc.setSumQueryTotalTime(q.getTimes());

        System.out.println("Queries Execution Time = " + tpchCalc.getQueryTotalTime());
    }

    private static void QphH(TPCHCalc tpchCalc, Connection con) {
        power(tpchCalc, con);
        throughput(tpchCalc, con);
    }

    public static void main(String[] args) throws SQLException {

        if (args.length > 3 || args.length == 0) {
            throw new IllegalArgumentException();
        }

        setSGBDParams(args[0]);
        db = "n";
        setSizeParams(args[1]);
        String host = "10.81.120.217";

        ConnectionFactory cf = new ConnectionFactory(sgbd, db, user, password, host, port);
        Connection con = cf.getConnection();
        System.out.println("Connection established with " + host + ": " + sgbd + " on " + scaleFactor + "GB normalized base");

        if (args.length == 3) {
            String operation = args[2];

            switch (operation) {
                case "-p":
                    power(new TPCHCalc(), con);
                    break;
                case "-t":
                    throughput(new TPCHCalc(), con);
                    break;
                case "-q":
                    queries(con);
                    break;
                case "-all":
                    TPCHCalc tpchCalc = new TPCHCalc();
                    QphH(tpchCalc, con);
                    break;
            }
        }
        else {
            TPCHCalc tpchCalc = new TPCHCalc();
            QphH(tpchCalc, con);
        }

        System.out.println("FINISHED!");        
        
        con.close();
    }
}
