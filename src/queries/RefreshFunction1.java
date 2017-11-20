/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queries;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author LTorres
 */
public class RefreshFunction1 {

    private Connection con;

    public RefreshFunction1() {
    }

    public RefreshFunction1(Connection con) {
        this.con = con;
    }

    public Connection getConnection() {
        return con;
    }

    public void setConnection(Connection con) {
        this.con = con;
    }

    public void update(int streamNo) {
        Statement st = null;
        try {
            st = this.con.createStatement();
            String sql = String.format("INSERT INTO orders SELECT * FROM rf1.orders%d", streamNo); // streamNo = 1, rf1.orders1
            st.executeUpdate(sql);
            sql = String.format("INSERT INTO lineitem SELECT * FROM rf1.lineitem%d;", streamNo);
            st.executeUpdate(sql);
        } catch (SQLException ex) {
            Logger.getLogger(RefreshFunction1.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                st.close();
            } catch (SQLException ex) {
                Logger.getLogger(RefreshFunction1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
