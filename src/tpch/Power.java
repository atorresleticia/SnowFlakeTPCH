/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tpch;

import java.sql.Connection;
import queries.RefreshFunctionTime;
import queries.TPCHQueriesTime;

/**
 *
 * @author LTorres
 */
public class Power {
    
    private final int sf;
    private final Connection con;
    private final TPCHQueriesTime tpchQT;
    private final RefreshFunctionTime rfT;
    private final String sgbd;

    public Power(Connection con, int sf, String sgbd) {
        this.sf = sf;
        this.con = con;
        this.sgbd = sgbd;
        this.tpchQT = new TPCHQueriesTime();
        this.rfT = new RefreshFunctionTime();
        setConnection();
    }

    public final void setConnection() {
        this.tpchQT.getTpchq().setConnection(this.con);
        this.rfT.getRF1().setConnection(this.con);
        this.rfT.getRF2().setConnection(this.con);
    }

    public void run() {
        String path = String.format("D:\\TPCH\\DATA\\ORIGINAL\\RF\\%dGB\\delete.1", this.sf);
        rfT.getRF2().getIndexes(path);
        
        rfT.runRF1();
        tpchQT.run();
        rfT.runRF2();

        saveTimes();
    }
    
    public void saveTimes(){
        String rfFileName = String.format("%s_SNOW_POWER_RF_%dGB", this.sgbd, this.sf);
        String queriesFileName = String.format("%s_SNOW_POWER_QUERIES_%dGB", this.sgbd, this.sf);
        
        rfT.saveRFTime(rfFileName);
        tpchQT.saveQueriesTime(queriesFileName);
    }

    public double[] getQueryExecutionTime() {
        return this.tpchQT.getTimes();
    }

    public double[] getRefreshExecutionTime() {
        return this.rfT.getTimes();
    }

}
