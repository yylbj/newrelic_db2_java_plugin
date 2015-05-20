package com.newrelic.plugins.db2;

import static com.newrelic.plugins.db2.util.Constants.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


import com.newrelic.metrics.publish.util.Logger;

/**
 * This class provide DB2 specific methods, operations and values for New Relic Agents reporting DB2 Metrics
 * 
 * @author yylbj@cn.ibm.com
 * 
 */
public class DB2 {

    private static final Logger logger = Logger.getLogger(DB2.class);
    
    private Connection conn = null; // Cached Database Connection
    private boolean connectionInitialized = false;

    public DB2() {
    }

    /**
     * This method will return a new DB2 database connection
     * 
     * @param host String Hostname for DB2 Connection
     * @param database String database name for DB2 connection
     * @param user String Database username for DB2 Connection
     * @param passwd String database password for DB2 Connection
     * @return connection new DB2 Connection
     */
    private Connection getNewConnection(String host, String database, String user, String passwd, String properties) {
        Connection newConn = null;
        //String dbURL = buildString(JDBC_URL, host, SLASH, database, SLASH, properties);
        String dbURL = buildString(JDBC_URL, host, SLASH, database);
        String connectionInfo = buildString(dbURL, SPACE, user, PASSWORD_FILTERED);

        logger.debug("Getting new DB2 Connection: ", connectionInfo);

        try {
            if (!connectionInitialized) {
                // load jdbc driver
                Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
                connectionInitialized = true;
            }
            newConn = DriverManager.getConnection(dbURL, user, passwd);
            if (newConn == null) {
                logger.error("Unable to obtain a new database connection: ", connectionInfo, ", check your DB2 configuration settings.");
            }
        } catch (Exception e) {
            logger.error("Unable to obtain a new database connection: ", connectionInfo, ", check your DB2 configuration settings. ", e.getMessage());
        }
        return newConn;
    }

    /**
     * This method will return a DB2 database connection for use, either a new connection or a cached connection
     * 
     * @param host String Hostname
     * @param database String Database name
     * @param user String Database username
     * @param passwd String database password
     * @return A DB2 Database connection for use
     */
    public Connection getConnection(String host, String database, String user, String passwd, String properties) {
        if (conn == null) {
            conn = getNewConnection(host, database, user, passwd, properties);
        }
        // Test Connection, and reconnect if necessary
        else if (!isConnectionValid()) {
            closeConnection();
            conn = getNewConnection(host, database, user, passwd, properties);
        }
        return conn;
    }

    /**
     * Check if connection is valid by pinging DB2 server. If connection is null or invalid return false, otherwise true.
     * 
     * @return the state of the connection
     */
    private boolean isConnectionValid() {
        boolean available = false;
        if (conn != null) {
            Statement stmt = null;
            ResultSet rs = null;
            try {
                logger.debug("Checking connection - pinging DB2 server");
                stmt = conn.createStatement();
                rs = stmt.executeQuery(PING);
                available = true;
            } catch (SQLException e) {
                logger.debug("The DB2 connection is not available.");
                available = false;
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (rs != null) {
                        rs.close();
                    }
                } catch (SQLException e) {
                    logger.debug(e, "Error closing statement/result set: ");
                }
                rs = null;
                stmt = null;
            }
        }
        return available;
    }

    /**
     * Close current connection
     */
    private void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException e) {
                logger.debug(e, "Error closing connection: ");
            }
        }
    }

    /**
     * 
     * This method will execute the given SQL Statement and produce a set of key/value pairs that are used for reporting metrics. This method is optimized for
     * queries designed to produce New Relic compatible type results
     * 
     * @param c Connection
     * @param SQL String of SQL Statement to execute
     * @return Map of key/value pairs
     */
    public static Map<String, Float> runSQL(Connection c, String category, String SQL, String type) {
        Statement stmt = null;
        ResultSet rs = null;
        Map<String, Float> results = new HashMap<String, Float>();

        try {
            logger.debug("Running SQL Statement ", SQL);
            stmt = c.createStatement();
            rs = stmt.executeQuery(SQL); // Execute the given SQL statement
            ResultSetMetaData md = rs.getMetaData(); // Obtain Meta data about the SQL query (column names etc)

            if (ROW.equals(type)) { // If we expect a single row of results
                if (rs.next()) {
                    for (int i = 1; i <= md.getColumnCount(); i++) { // use column names as the "key"
                        String value = transformStringMetric(rs.getString(i));
                        String columnName = md.getColumnName(i).toLowerCase();
                        if (validMetricValue(value)) {
                            String key = buildString(category, SEPARATOR, columnName);
                            results.put(key, translateStringToNumber(value));
                        }
                        
                    }
                }
            } else if (SET.equals(type)) { //currently support bufferpool,tablespace utilization and HADR report
            	String firstColumnName = md.getColumnName(1);
            	if(firstColumnName.equalsIgnoreCase(TBSP_COLUMN_NAME) ||
            	   firstColumnName.equalsIgnoreCase(BP_COLUMN_NAME) ||
            	   firstColumnName.equalsIgnoreCase(HADR_COLUMN_NAME)){
            		 while (rs.next()) {
            			//The format of key for each bufferpool & tablespace is like: bufferpool_MYBP
            			String newCategory = category + UNDERSCORE + rs.getString(1);
                     	for (int i = 2; i <= md.getColumnCount(); i++) { // use column names as the "key"
                     		String value = transformStringMetric(rs.getString(i));
                            String columnName = md.getColumnName(i).toLowerCase();
                            if (validMetricValue(value)) {
                                String key = buildString(newCategory, SEPARATOR, columnName);
                                results.put(key, translateStringToNumber(value));
                            }
                     	}//for                      
                     } //while
                 }//if
            }       
            return results;
        } catch (SQLException e) {
            logger.error("An SQL error occured running '", SQL, "' ", e.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close(); // Release objects
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                ;
            }
            rs = null;
            stmt = null;
        }
        return results;
    }
    /**
     * This method will convert the provided string into a Number (either int or float)
     * 
     * @param String value to convert
     * @return Number A int or float representation of the provided string
     */
    public static Float translateStringToNumber(String val) {
        try {
            if (val.contains(SPACE)) {
                val = SPACE_PATTERN.matcher(val).replaceAll(EMPTY_STRING); // Strip any spaces
            }
            return Float.parseFloat(val);
        } catch (Exception e) {
            logger.error("Unable to parse int/float number from value ", val);
        }
        return 0.0f;
    }

    /**
     * Perform some preliminary transformation of string values that can be represented in integer values for monitoring
     * 
     * @param val String value to evaluate
     * @return String value that best represents and integer
     */
    static String transformStringMetric(String val) {
        if (ON.equalsIgnoreCase(val) || TRUE.equalsIgnoreCase(val)) return ONE; // Convert some TEXT metrics into numerics
        if (OFF.equalsIgnoreCase(val) || NONE.equalsIgnoreCase(val)) return ZERO;
        if (YES.equalsIgnoreCase(val)) return ONE; // For slave/slave_*_running
        if (NO.equalsIgnoreCase(val)) return ZERO; // For slave/slave_*_running
        if (NULL.equalsIgnoreCase(val)) return NEG_ONE; // For slave/seconds_behind_master
        
        //for HADR metrics string transform
        if (DISCONNECTED.equalsIgnoreCase(val)) return ZERO;
        if (LOCAL_CATCHUP.equalsIgnoreCase(val)) return ONE;
        if (REMOTE_CATCHUP_PENDING.equalsIgnoreCase(val)) return THREE;
        if (REMOTE_CATCHUP.equalsIgnoreCase(val)) return FOUR;
        if (PEER.equalsIgnoreCase(val)) return FIVE;
        if (CONNECTED.equalsIgnoreCase(val)) return ONE;
        if (CONGESTED.equalsIgnoreCase(val)) return TWO;
        return val;
    }

    /**
     * Check if the value is a valid New Relic Metric value
     * 
     * @param val String to validate
     * @return TRUE if string is a numeric supported by New Relic
     */
    static boolean validMetricValue(String val) {
        if (val == null || EMPTY_STRING.equals(val)) {
            return false;
        }
        if (VALID_METRIC_PATTERN.matcher(val).matches()) {
            return true;
        }
        return false;
    }

    static String buildString(String... strings) {
        StringBuilder builder = new StringBuilder(50);
        for (String string : strings) {
        	if(string == null || EMPTY_STRING.equals(string)){
        		continue;
        	}
            builder.append(string);
        }
        return builder.toString();
    }
}
