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

/**
 *
 * @author LTorres
 */
public class RefreshFunction2 {

    private ArrayList indexesToDelete;
    private Connection con;

    public RefreshFunction2() {
        this.indexesToDelete = new ArrayList();
    }

    public RefreshFunction2(ArrayList indexedToDelete) {
        this.indexesToDelete = indexedToDelete;
    }

    public RefreshFunction2(Connection con) {
        this.con = con;
        this.indexesToDelete = new ArrayList();
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
            br = new BufferedReader(new FileReader(path));
            String ptr;
            while ((ptr = br.readLine()) != null) {
                this.indexesToDelete.add(Integer.parseInt(ptr));
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
            for (int i = 0; i < this.indexesToDelete.size(); i++) {
                String sql = String.format("DELETE FROM lineitem WHERE l_orderkey = %d", this.indexesToDelete.get(i));
                st.executeUpdate(sql);      
                sql = String.format("DELETE FROM orders WHERE o_orderkey = %d", this.indexesToDelete.get(i));
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
