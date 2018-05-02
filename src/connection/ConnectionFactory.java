/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author LTorres
 */
public class ConnectionFactory {
    
    private final String url;
    private final String user;
    private final String password;

    public ConnectionFactory(String sgbd, String db, String user, String password, String host, String port) {
        this.url = String.format("jdbc:%s://%s:%s/%s", sgbd, host, port, db);
        this.user = user;
        this.password = password;
    }

    public ConnectionFactory(String sgbd, String db, String user, String password, String host) {
        this.url = "jdbc:" + sgbd + "://" + host + "/" + db;
        this.user = user;
        this.password = password;
    }

    public ConnectionFactory(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }
    
    public Connection getConnection(){
        
        Connection con = null;

        try {
            con = DriverManager.getConnection(url, user, password);
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return con;
    }   
    
}
