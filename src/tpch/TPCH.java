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

    private static void setModelingParams(String modeling) {
        if (modeling.equals("n")) {
            db = "n";
        }
        else if (modeling.equals("d")) {
            db = "d";
        }
    }

    private static void setSizeParams(String size) {
        if (size.equals("1")) {
            scaleFactor = "1";
            db += "1";
        }
        else if (size.equals("10")) {
            scaleFactor = "10";
            db += "10" ;
        }
        else if (size.equals("30")) {
            scaleFactor = "30";
            db += "30";
        }
    }

    private static void power(TPCHCalc tpchCalc, Connection con) {

        System.out.println("Power running...");
        Power p = new Power(con, Integer.valueOf(scaleFactor), sgbd);
        p.run();

        tpchCalc.setSumQueryTotalTime(p.getQueryExecutionTime());
        tpchCalc.setSumRefreshTotalTime(p.getRefreshExecutionTime());
        System.out.println("Power Test Execution Time = " + (tpchCalc.getQueryTotalTime() + tpchCalc.getRefreshTotalTime()));

        tpchCalc.setQueryTotalTime(p.getQueryExecutionTime());
        tpchCalc.setRefreshTotalTime(p.getRefreshExecutionTime());
        tpchCalc.setPowerQph(Integer.valueOf(scaleFactor));
        System.out.println("Power@Size (QPH) = " + tpchCalc.getPowerQph());
    }

    private static void throughput(TPCHCalc tpchCalc, Connection con) {

        System.out.println("Throughput running...");
        Throughput t = new Throughput(con, Integer.valueOf(scaleFactor), sgbd);
        t.run();

        tpchCalc.setThroughputQph(Integer.valueOf(scaleFactor), t.getNoOfStreams(), t.getTime());

        System.out.println("Throughput Test Execution Time = " + t.getTime());
        System.out.println("Throuthput@Size (QPH) = " + tpchCalc.getThroughputQph());
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

        tpchCalc.setQphH();

        System.out.println("QphH@Size = " + tpchCalc.getQphH());
    }

    public static void main(String[] args) throws SQLException {

        if (args.length > 4 || args.length == 0) {
            throw new IllegalArgumentException();
        }

        setSGBDParams(args[0]);
        setModelingParams(args[1]);
        setSizeParams(args[2]);

        System.out.println(args[0] + " " + args[1] + " " + args[2] + " " + args[3]);
        String host = "10.81.120.217";

        ConnectionFactory cf = new ConnectionFactory(sgbd, db, user, password, host, port);
        Connection con = cf.getConnection();
        System.out.println("Connection established with " + host + ": " + sgbd + " on " + db + " base");

        if (args.length == 4) {
            String operation = args[3];

            if (operation.equals("-p")) {
                power(new TPCHCalc(), con);
            }
            else if (operation.equals("-t")) {
                throughput(new TPCHCalc(), con);
            }
            else if (operation.equals("-q")) {
                queries(con);
            }
            else if (operation.equals("-all")) {
                TPCHCalc tpchCalc = new TPCHCalc();
                QphH(tpchCalc, con);
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
