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
public class RefreshFunctionTime {

    private final RefreshFunction1 rf1;
    private final RefreshFunction2 rf2;
    private final double[] rfTimes;

    public RefreshFunctionTime() {
        this.rf1 = new RefreshFunction1();
        this.rf2 = new RefreshFunction2();
        this.rfTimes = new double[2];
    }

    public RefreshFunction1 getRF1() {
        return rf1;
    }    
    
    public RefreshFunction2 getRF2() {
        return rf2;
    }   
    
    private double getRF1Time() {
        long startTime = System.nanoTime();
        rf1.update(1);
        long endTime = System.nanoTime();

        System.out.println("RF1 done.");

        return (endTime - startTime) / 1000000.;
    }

    private double getRF2Time() {
               
        long startTime = System.nanoTime();
        rf2.delete();
        long endTime = System.nanoTime();

        System.out.println("RF2 done.");

        return (endTime - startTime) / 1000000.;
    }
    
    public void runRF1(){
        this.rfTimes[0] = getRF1Time();
    }
    
    public void runRF2(){
        this.rfTimes[1] = getRF2Time();
    }

    public double[] getTimes() {
        return this.rfTimes;
    }
    
    public void saveRFTime(String file){
        try (BufferedWriter queriesTimesIO = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)))) { 
            
            queriesTimesIO.write(Arrays.toString(this.rfTimes));
            queriesTimesIO.newLine();
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TPCHQueriesTime.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RefreshFunctionTime.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
