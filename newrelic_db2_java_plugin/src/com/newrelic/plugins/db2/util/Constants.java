package com.newrelic.plugins.db2.util;

import java.util.regex.Pattern;

public class Constants {

    public static final String COMMA = ",";
    public static final String SLASH = "/";
    public static final String SPACE = " ";
    public static final String EMPTY_STRING = "";
    public static final String UNDERSCORE = "_";
    public static final String LEFT_PAREN = "(";
    public static final String RIGHT_PAREN = ")";
    public static final String ARROW = "->";
    public static final String EQUALS = "=";
    public static final String NEW_LINE = "\n"; 
    public static final String SQL = "SQL";
    public static final String RESULT = "result";
    public static final String COUNTER = "[counter]";
    public static final String METRIC_LOG_PREFIX = "Metric ";

    public static final String SEPARATOR = "/";
    public static final String PING = "SELECT 1 from sysibm.sysdummy1";
    public static final Pattern VALID_METRIC_PATTERN = Pattern.compile("(-)?(\\.)?\\d+(\\.\\d+)?");  // Only integers and floats are valid metric values
    public static final Pattern SPACE_PATTERN = Pattern.compile(" ");

    public static final String JDBC_URL = "jdbc:db2://";
    public static final String PASSWORD_FILTERED = "/PASSWORD_FILTERED";

    public static final String ROW = "row";
    public static final String SET = "set";
    public static final String SPECIAL = "special";

    public static final String ON = "ON";
    public static final String OFF = "OFF";
    public static final String TRUE = "TRUE";
    public static final String NONE = "NONE";
    public static final String YES = "YES";
    public static final String NO = "NO";
    public static final String NULL = "NULL";
    //For HADR state & connect_status metrics transform
    public static final String DISCONNECTED= "DISCONNECTED";
    public static final String LOCAL_CATCHUP= "LOCAL_CATCHUP";
    public static final String REMOTE_CATCHUP_PENDING= "REMOTE_CATCHUP_PENDING";
    public static final String REMOTE_CATCHUP= "REMOTE_CATCHUP";
    public static final String PEER= "PEER";
    public static final String DISCONNECTED_PEER= "DISCONNECTED_PEER";
    public static final String CONNECTED= "CONNECTED";
    public static final String CONGESTED= "CONGESTED";
   
    public static final String ONE = "1";
    public static final String NEG_ONE = "-1";
    public static final String ZERO = "0";
    public static final String TWO = "2";
    public static final String THREE = "3";
    public static final String FOUR = "4";
    public static final String FIVE = "5";
    
    public static final String CONNECTION_CATEGORY = "connection";
    public static final String OVERVIEW_CATEGORY = "overview";
   
    public static final String DEFAULT_UNIT = "";
    public static final String STATEMENTS_UNIT = "Statements";
    public static final String ACTIVITIES_UNIT = "Activities";
    public static final String REQUESTS_UNIT = "Requests";
    public static final String TIME_UNIT = "Microseconds";
    public static final String PERCENTAGE_UNIT = "%";
    public static final String TIMES_UNIT = "Times";
    
    public static final String TBSP_COLUMN_NAME = "TBSP_NAME";
    public static final String BP_COLUMN_NAME = "BP_NAME";
    public static final String HADR_COLUMN_NAME = "STANDBY_ID";
    
}
