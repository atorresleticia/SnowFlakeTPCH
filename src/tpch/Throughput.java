/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tpch;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import queries.RefreshFunction1;
import queries.RefreshFunction2;
import queries.TPCHQueries;
import queries.TPCHQueriesTime;

/**
 *
 * @author LTorres
 */
public class Throughput {

    private final int sf;
    private final Connection con;
    private final RefreshFunction1 rf1;
    private final RefreshFunction2[] rf2;
    private final TPCHQueries tpchq;
    private Map<Integer, Integer> stream;
    private String sgbd;
    private double time;

    public Throughput(Connection con, int sf, String sgbd) {
        initMap();
        this.sf = sf;
        this.con = con;
        this.rf1 = new RefreshFunction1();
        this.rf2 = new RefreshFunction2[getNoOfStreams()]; // retorna o numero de streams para o sf
        this.tpchq = new TPCHQueries();
        this.sgbd = sgbd;

        setConnection();
    }

    public final void initMap() {
        this.stream = new HashMap<>();

        this.stream.put(1, 2);
        this.stream.put(10, 3);
        this.stream.put(30, 4);

    }

    public final int getNoOfStreams() {
        return this.stream.get(this.sf);
    }

    public final void setConnection() {
        this.rf1.setConnection(this.con);

        for (int i = 0; i < getNoOfStreams(); i++) {
            this.rf2[i] = new RefreshFunction2();
            this.rf2[i].setConnection(this.con);
            this.rf2[i].getIndexes(String.format("D:\\TPCH\\DATA\\ORIGINAL\\RF\\%dGB\\delete.%d", this.sf, i + 1));
        }

        this.tpchq.setConnection(this.con);
    }

    public void sf1Stream() {

        Runnable r1 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 1 finished.");
        };

        Runnable r2 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 2 finished.");
        };

        Runnable r3 = () -> {
            for (int i = 0; i < getNoOfStreams(); i++) {
                rf1.update(i + 1);
                rf2[i].delete();
            }

            System.out.println("RF stream finished.");
        };

        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);
        Thread t3 = new Thread(r3);

        t1.start();
        t2.start();
        t3.start();

        long startTime = System.nanoTime();

        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Throughput.class.getName()).log(Level.SEVERE, null, ex);
        }

        long endTime = System.nanoTime();

        this.time = (endTime - startTime) / 1000000.;
        saveTime();
    }

    public void sf10Stream() {

        Runnable r1 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 1 finished.");
        };

        Runnable r2 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 2 finished.");
        };

        Runnable r3 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 3 finished.");
        };

        Runnable r4 = () -> {
            for (int i = 0; i < getNoOfStreams(); i++) {
                rf1.update(i + 1);
                rf2[i].delete();
            }

            System.out.println("RF stream finished.");
        };

        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);
        Thread t3 = new Thread(r3);
        Thread t4 = new Thread(r4);

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        long startTime = System.nanoTime();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Throughput.class.getName()).log(Level.SEVERE, null, ex);
        }

        long endTime = System.nanoTime();

        this.time = (endTime - startTime) / 1000000.;
        saveTime();
    }

    public void sf30Stream() {

        Runnable r1 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 1 finished.");
        };

        Runnable r2 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 2 finished.");
        };

        Runnable r3 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 3 finished.");
        };

        Runnable r4 = () -> {
            tpchq.runQueries();
            System.out.println("Query stream 4 finished.");
        };

        Runnable r5 = () -> {
            for (int i = 0; i < getNoOfStreams(); i++) {
                rf1.update(i + 1);
                rf2[i].delete();
            }

            System.out.println("RF stream finished.");
        };

        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);
        Thread t3 = new Thread(r3);
        Thread t4 = new Thread(r4);
        Thread t5 = new Thread(r5);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        long startTime = System.nanoTime();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
            t5.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Throughput.class.getName()).log(Level.SEVERE, null, ex);
        }

        long endTime = System.nanoTime();

        this.time = (endTime - startTime) / 1000000.;
        saveTime();
    }

    public double getTime() {
        return this.time;
    }

    public void saveTime() {
        String file = String.format("%s_SNOW_THROUGHPUT_%dGB", this.sgbd, this.sf);

        try (BufferedWriter queriesTimesIO = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)))) {
            queriesTimesIO.write(Double.toString(this.time));
            queriesTimesIO.newLine();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TPCHQueriesTime.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(TPCHQueriesTime.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void run() {
        switch (this.sf) {
            case 1:
                sf1Stream();
                break;
            case 10:
                sf10Stream();
                break;
            case 30:
                sf30Stream();
                break;
            default:
                System.out.println("Invalid scale factor. Possibilities are 1, 10 or 30.");
                break;
        }
    }
}
