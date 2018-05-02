/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queries;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author LTorres
 */
public class TPCHQueriesTime {

    private final TPCHQueries tpchq;
    private final double[] tpchTimes;

    public TPCHQueriesTime() {
        this.tpchq = new TPCHQueries();
        this.tpchTimes = new double[15];
    }

    public TPCHQueries getTpchq() {
        return this.tpchq;
    }

    public void run() {
        tpchTimes[0] = this.getQuery3Time();
        tpchTimes[1] = this.getQuery5Time();
        tpchTimes[2] = this.getQuery6Time();
        tpchTimes[3] = this.getQuery7Time();
        tpchTimes[4] = this.getQuery8Time();
        tpchTimes[5] = this.getQuery9Time();
        tpchTimes[6] = this.getQuery10Time();
        tpchTimes[7] = this.getQuery11Time();
        tpchTimes[8] = this.getQuery12Time();
        tpchTimes[9] = this.getQuery13Time();
        tpchTimes[10] = this.getQuery14Time();
        tpchTimes[11] = this.getQuery16Time();
        tpchTimes[12] = this.getQuery18Time();
        tpchTimes[13] = this.getQuery19Time();
        tpchTimes[14] = this.getQuery22Time();
    }

    public double[] getTimes() {
        return this.tpchTimes;
    }

    public void saveQueriesTime(String file) {
        try (BufferedWriter queriesTimesIO = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)))) {
            queriesTimesIO.write(Arrays.toString(tpchTimes));
            queriesTimesIO.newLine();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TPCHQueriesTime.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(TPCHQueriesTime.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public double getQuery1Time() {

        long startTime = System.nanoTime();
        tpchq.query1();
        long endTime = System.nanoTime();
        
        System.out.println("Query 1 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery2Time() {

        long startTime = System.nanoTime();
        tpchq.query2();
        long endTime = System.nanoTime();

        System.out.println("Query 2 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery3Time() {

        long startTime = System.nanoTime();
        tpchq.query3();
        long endTime = System.nanoTime();

        System.out.println("Query 3 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery4Time() {

        long startTime = System.nanoTime();
        tpchq.query4();
        long endTime = System.nanoTime();

        System.out.println("Query 4 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery5Time() {

        long startTime = System.nanoTime();
        tpchq.query5();
        long endTime = System.nanoTime();

        System.out.println("Query 5 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery6Time() {

        long startTime = System.nanoTime();
        tpchq.query6();
        long endTime = System.nanoTime();

        System.out.println("Query 6 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery7Time() {

        long startTime = System.nanoTime();
        tpchq.query7();
        long endTime = System.nanoTime();

        System.out.println("Query 7 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery8Time() {

        long startTime = System.nanoTime();
        tpchq.query8();
        long endTime = System.nanoTime();

        System.out.println("Query 8 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery9Time() {

        long startTime = System.nanoTime();
        tpchq.query9();
        long endTime = System.nanoTime();

        System.out.println("Query 9 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery10Time() {

        long startTime = System.nanoTime();
        tpchq.query10();
        long endTime = System.nanoTime();

        System.out.println("Query 10 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery11Time() {

        long startTime = System.nanoTime();
        tpchq.query11();
        long endTime = System.nanoTime();

        System.out.println("Query 11 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery12Time() {

        long startTime = System.nanoTime();
        tpchq.query12();
        long endTime = System.nanoTime();

        System.out.println("Query 12 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery13Time() {

        long startTime = System.nanoTime();
        tpchq.query13();
        long endTime = System.nanoTime();

        System.out.println("Query 13 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery14Time() {

        long startTime = System.nanoTime();
        tpchq.query14();
        long endTime = System.nanoTime();

        System.out.println("Query 14 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery15Time() {

        long startTime = System.nanoTime();
        tpchq.query15();
        long endTime = System.nanoTime();

        System.out.println("Query 15 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery16Time() {

        long startTime = System.nanoTime();
        tpchq.query16();
        long endTime = System.nanoTime();

        System.out.println("Query 16 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery17Time() {

        long startTime = System.nanoTime();
        tpchq.query17();
        long endTime = System.nanoTime();

        System.out.println("Query 17 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery18Time() {

        long startTime = System.nanoTime();
        tpchq.query18();
        long endTime = System.nanoTime();

        System.out.println("Query 18 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery19Time() {

        long startTime = System.nanoTime();
        tpchq.query19();
        long endTime = System.nanoTime();

        System.out.println("Query 19 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery20Time() {

        long startTime = System.nanoTime();
        tpchq.query20();
        long endTime = System.nanoTime();

        System.out.println("Query 20 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery21Time() {

        long startTime = System.nanoTime();
        tpchq.query21();
        long endTime = System.nanoTime();

        System.out.println("Query 21 done.");
        
        return (endTime - startTime) / 1000000.;
    }

    public double getQuery22Time() {

        long startTime = System.nanoTime();
        tpchq.query22();
        long endTime = System.nanoTime();

        System.out.println("Query 22 done.");
        
        return (endTime - startTime) / 1000000.;
    }
}
