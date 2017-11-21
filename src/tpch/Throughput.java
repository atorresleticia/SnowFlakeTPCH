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
import java.util.Arrays;
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

    public Throughput(int sf, Connection con) {
        initMap();
        this.sf = sf;
        this.con = con;
        this.rf1 = new RefreshFunction1();
        this.rf2 = new RefreshFunction2[getNoOfStreams()]; // retorna o numero de streams para o sf
        this.tpchq = new TPCHQueries();

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
            this.rf2[i].setConnection(this.con);
            this.rf2[i].getIndexes(String.format("D:\\TPCH\\DATA\\ORIGINAL\\RF\\%dGB\\delete.%d", this.sf, i + 1));
        }

        this.tpchq.setConnection(this.con);
    }

    public void sf1Stream() {

        // Thread for stream 1
        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 1 finished.");

        }).start();

        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 2 finished.");

        }).start();

        new Thread(() -> {

            rf1.update(1);
            rf2[0].delete();

            rf1.update(2);
            rf2[1].delete();

            System.out.println("RF stream finished.");

        }).start();
    }

    public void sf10Stream() {

        // Thread for stream 1
        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 1 finished.");

        }).start();

        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 2 finished.");

        }).start();

        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 3 finished.");

        }).start();

        new Thread(() -> {

            rf1.update(1);
            rf2[0].delete();

            rf1.update(2);
            rf2[1].delete();

            rf1.update(3);
            rf2[2].delete();

            System.out.println("RF stream finished.");

        }).start();
    }

    public void sf30Stream() {

        // Thread for stream 1
        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 1 finished.");

        }).start();

        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 2 finished.");

        }).start();

        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 3 finished.");

        }).start();

        new Thread(() -> {

            tpchq.runQueries();
            System.out.println("Query stream 4 finished.");

        }).start();

        new Thread(() -> {

            rf1.update(1);
            rf2[0].delete();

            rf1.update(2);
            rf2[1].delete();

            rf1.update(3);
            rf2[2].delete();

            rf1.update(4);
            rf2[3].delete();

            System.out.println("RF stream finished.");

        }).start();
    }

    public void sf1StreamTime() {

        long startTime = System.nanoTime();
        sf1Stream();
        long endTime = System.nanoTime();

        saveTime((endTime - startTime) / 1000000.);
    }

    public void sf10StreamTime() {

        long startTime = System.nanoTime();
        sf10Stream();
        long endTime = System.nanoTime();

        saveTime((endTime - startTime) / 1000000.);
    }

    public void sf30StreamTime() {

        long startTime = System.nanoTime();
        sf30Stream();
        long endTime = System.nanoTime();

        System.out.println("RF1 done.");

        saveTime((endTime - startTime) / 1000000.);
    }

    public void saveTime(double time) {
        String file = String.format("SNOW_THROUGHPUT_%dGB", this.sf);

        try (BufferedWriter queriesTimesIO = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)))) {
            queriesTimesIO.write(Double.toString(time));
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
                sf1StreamTime();
                break;
            case 10:
                sf10StreamTime();
                break;
            case 30:
                sf30StreamTime();
                break;
            default:
                System.out.println("Invalid scale factor. Possibilities are 1, 10 or 30.");
                break;
        }
    }
}
