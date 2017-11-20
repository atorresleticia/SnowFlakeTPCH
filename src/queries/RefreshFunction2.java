/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queries;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;

/**
 *
 * @author LTorres
 */
public class RefreshFunction2 {

    private ArrayList indexedToDelete;
    private Connection con;

    public RefreshFunction2() {
    }

    public RefreshFunction2(ArrayList indexedToDelete) {
        this.indexedToDelete = indexedToDelete;
    }

    public RefreshFunction2(Connection con) {
        this.con = con;
        this.indexedToDelete = new ArrayList();
    }

    public Connection getConnection() {
        return con;
    }

    public void setConnection(Connection con) {
        this.con = con;
    }

    public void getIndexes(String path) {
        BufferedReader br = null;

        try {
            br = new BufferedMCLReader(new FileReader(path));
            String currentInt;
            int i = 0;
            while ((currentInt = br.readLine()) != null) {
                this.indexedToDelete.add(Integer.parseInt(currentInt));
                System.out.println(i + " - " + this.indexedToDelete.get(i));
                i++;
            }

        } catch (FileNotFoundException ex) {
            Logger.getLogger(RefreshFunction2.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RefreshFunction2.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(RefreshFunction2.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void delete() {
        Statement st = null;
        try {
            st = this.con.createStatement();
            for (int i = 0; i < this.indexedToDelete.size(); i++) {
                String sql = String.format("DELETE FROM lineitem WHERE l_orderkey = %d", this.indexedToDelete.get(i));
                st.executeQuery(sql);
                sql = String.format("DELETE FROM orders WHERE o_orderkey = %d", this.indexedToDelete.get(i));
                st.executeUpdate(sql);
            }
        } catch (SQLException ex) {
            Logger.getLogger(RefreshFunction2.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                st.close();
            } catch (SQLException ex) {
                Logger.getLogger(RefreshFunction2.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
