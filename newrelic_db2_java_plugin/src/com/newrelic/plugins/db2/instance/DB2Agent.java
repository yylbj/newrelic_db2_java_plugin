package com.newrelic.plugins.db2.instance;

import static com.newrelic.plugins.db2.util.Constants.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.util.Logger;
import com.newrelic.plugins.db2.MetricMeta;
import com.newrelic.plugins.db2.DB2;

/**
 * This class creates a specific DB2 agent that is used to obtain a DB2 database connection, 
 * gather requested metrics and report to New Relic
 * 
 * @author yylbj@cn.ibm.com
 * 
 */
public class DB2Agent extends Agent {
    
    private static final Logger logger = Logger.getLogger(DB2Agent.class);
    
    private static final String GUID = "com.newrelic.plugins.DB2.instance";
    private static final String version = "1.0.0";

    public static final String AGENT_DEFAULT_HOST = "localhost:50000"; // Default values for DB2 Agent
    public static final String AGENT_DEFAULT_METRICS = "overview";  //////////////new to update when category is ready

    private final String name; // Agent Name

    private final String host; // DB2 Connection parameters
    private final String database;
    private final String user;
    private final String passwd;
    private final String properties;
    private String agentInfo;

    private final Set<String> metrics;
    // Definition of DB2 meta data (counter, unit, type etc)
    private final Map<String, MetricMeta> metricsMeta = new HashMap<String, MetricMeta>();
    // Definition of categories of metrics
    private Map<String, Object> metricCategories = new HashMap<String, Object>();

    private final DB2 m; // Per agent DB2 Object

    private boolean firstReport = true;

    /**
     * Default constructor to create a new DB2 Agent
     * 
     * @param map
     * 
     * @param String Human name for Agent
     * @param String DB2 Instance host:port
     * @param String DB2 user
     * @param String DB2 user password
     * @param String CSVm List of metrics to be monitored
     */
    public DB2Agent(String name, String host, String database, String user, String passwd, String properties, Set<String> metrics, Map<String, Object> metricCategories) {
        super(GUID, version);

        this.name = name; // Set local attributes for new class object
        this.host = host;
        this.database = database;
        this.user = user;
        this.passwd = passwd;
        this.properties = properties;

        this.metrics = metrics;
        this.metricCategories = metricCategories;

        this.m = new DB2();

        createMetaData(); // Define incremental counters that are value/sec etc

        logger.debug("DB2 Agent initialized: ", formatAgentParams(name, host, database, user, properties, metrics));
    }

    /**
     * Format Agent parameters for logging
     * 
     * @param name
     * @param host
     * @param user
     * @param properties
     * @param metrics
     * @return A formatted String representing the Agent parameters
     */
    private String formatAgentParams(String name, String host, String database, String user, String properties, Set<String> metrics) {
        StringBuilder builder = new StringBuilder();
        builder.append("name: ").append(name).append(" | ");
        builder.append("host: ").append(host).append(" | ");
        builder.append("database: ").append(database).append(" | ");
        builder.append("user: ").append(user).append(" | ");
        builder.append("properties: ").append(properties).append(" | ");
        builder.append("metrics: ").append(metrics).append(" | ");
        return builder.toString();
    }

    /**
     * This method is run for every poll cycle of the Agent. Get a DB2 Database connection and gather metrics.
     */
    @Override
    public void pollCycle() {
        Connection c = m.getConnection(host, database, user, passwd, properties); // Get a database connection (which should be cached)
        if (c == null) {
            return; // Unable to continue without a valid database connection
        }

        logger.debug("Gathering DB2 metrics. ", getAgentInfo());

        Map<String, Float> results = gatherMetrics(c); // Gather defined metrics
        reportMetrics(results); // Report Metrics to New Relic
        firstReport = false;
    }

    /**
     * This method runs the varies categories of DB2 statements and gathers the metrics that can be reported
     * 
     * @param Connection c DB2 Database Connection
     * @param String List of metrics to be obtained for this agent
     * @return Map of metrics and values
     */
    private Map<String, Float> gatherMetrics(Connection c) {
        Map<String, Float> results = new HashMap<String, Float>(); // Create an empty set of results
        Map<String, Object> categories = getMetricCategories(); // Get current Metric Categories

        Iterator<String> iter = categories.keySet().iterator();
        while (iter.hasNext()) {
            String category = iter.next();
            @SuppressWarnings("unchecked")
            Map<String, String> attributes = (Map<String, String>) categories.get(category);
            if (isReportingForCategory(category)) {
                results.putAll(DB2.runSQL(c, category, attributes.get(SQL), attributes.get(RESULT)));
            }
        }
        results.putAll(newRelicMetrics(results));
        return results;
    }

    /**
     * This method creates a number of custom New Relic Metrics, that are derived from raw DB2 status metrics
     * 
     * @param Map existing Gathered DB2 metrics
     * @param metrics String of the Metric Categories to capture
     * @return Map Additional derived metrics
     */
    protected Map<String, Float> newRelicMetrics(Map<String, Float> existing) {
        Map<String, Float> derived = new HashMap<String, Float>();

        if (!isReportingForCategory(OVERVIEW_CATEGORY)) {
            return derived; // "overview" category is a pre-requisite for newrelic metrics
        }

        logger.debug("End of newRelicMetrics, at this time, nothing is added to newRelicMetrics");

        return derived;
    }

    /**
     * This method does the reporting of metrics to New Relic
     * 
     * @param Map results
     */
    public void reportMetrics(Map<String, Float> results) {
        int count = 0;
        logger.debug("Collected ", results.size(), " DB2 metrics. ", getAgentInfo());
        logger.debug(results);

        Iterator<String> iter = results.keySet().iterator();
        while (iter.hasNext()) { // Iterate over current metrics
            String key = iter.next();
            Float val = results.get(key);
            MetricMeta md = getMetricMeta(key);
            if (md != null) { // Metric Meta data exists (from metric.category.json)
                logger.debug(METRIC_LOG_PREFIX, key, SPACE, md, EQUALS, val);
                count++;

                if (md.isCounter()) { // Metric is a counter
                    reportMetric(key, md.getUnit(), md.getCounter().process(val));
                } else { // Metric is a fixed Number
                    reportMetric(key, md.getUnit(), val);
                }
            } else { // md != null
            	/*
                if (firstReport) { // Provide some feedback of available metrics for future reporting
                    logger.debug("Not reporting identified metric ", key);
                }
                */
            	logger.debug("Meta for metrics ", key, " doesn't exist, using default unit Operations, the value is:", val);
            	 reportMetric(key, DEFAULT_UNIT, val);
            }
        }
        logger.debug("Reported to New Relic ", count, " metrics. ", getAgentInfo());
    }

    /**
     * Is this agent reporting metrics for a specific category
     * 
     * @param metricCategory
     * @return boolean
     */
    boolean isReportingForCategory(String metricCategory) {
        return metrics.contains(metricCategory);
    }

    private String getAgentInfo() {
        if (agentInfo == null) {
            agentInfo = new StringBuilder().append("Agent Name: ").append(name).append(". Agent Version: ").append(version).toString();
        }
        return agentInfo;
    }

    /**
     * This method creates the metric meta data that is derived from the provided configuration and New Relic specific metrics.
     */
    private void createMetaData() {

        Map<String, Object> categories = getMetricCategories(); // Get current Metric Categories
        Iterator<String> iter = categories.keySet().iterator();
        while (iter.hasNext()) {
            String category = iter.next();
            @SuppressWarnings("unchecked")
            Map<String, String> attributes = (Map<String, String>) categories.get(category);
            String valueMetrics = attributes.get("value_metrics");
            if (valueMetrics != null) {
                Set<String> metrics = new HashSet<String>(Arrays.asList(valueMetrics.toLowerCase().replaceAll(SPACE, EMPTY_STRING).split(COMMA)));
                for (String s : metrics) {
                    addMetricMeta(category + SEPARATOR + s, new MetricMeta(false));
                }

            }
            String counterMetrics = attributes.get("counter_metrics");
            if (counterMetrics != null) {
                Set<String> metrics = new HashSet<String>(Arrays.asList(counterMetrics.toLowerCase().replaceAll(SPACE, EMPTY_STRING).split(COMMA)));
                for (String s : metrics) {
                    addMetricMeta(category + SEPARATOR + s, new MetricMeta(true));
                }
            }
        }

        //Define overview metrics meta data
        addMetricMeta("overview/TOTAL_APP_COMMITS", new MetricMeta(false, STATEMENTS_UNIT));
        addMetricMeta("overview/TOTAL_APP_ROLLBACKS", new MetricMeta(false, STATEMENTS_UNIT));
        addMetricMeta("overview/ACT_COMPLETED_TOTAL", new MetricMeta(false, ACTIVITIES_UNIT));
        addMetricMeta("overview/APP_RQSTS_COMPLETED_TOTAL", new MetricMeta(false, REQUESTS_UNIT));
        addMetricMeta("overview/AVG_RQST_CPU_TIME", new MetricMeta(false, TIME_UNIT));
        addMetricMeta("overview/ROUTINE_TIME_RQST_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/RQST_WAIT_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/ACT_WAIT_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/IO_WAIT_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/LOCK_WAIT_TIME_ PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/AGENT_WAIT_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/NETWORK_WAIT_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/SECTION_PROC_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/SECTION_SORT_PROC_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/COMPILE_PROC_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/TRANSACT_END_PROC_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/UTILS_PROC_TIME_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
        addMetricMeta("overview/AVG_LOCK_WAITS_PER_ACT", new MetricMeta(false, TIMES_UNIT));
        addMetricMeta("overview/AVG_LOCK_TIMEOUTS_PER_ACT", new MetricMeta(false, TIMES_UNIT));
        addMetricMeta("overview/AVG_DEADLOCKS_PER_ACT", new MetricMeta(false, DEFAULT_UNIT));
        addMetricMeta("overview/AVG_LOCK_ESCALS_PER_ACT", new MetricMeta(false, TIMES_UNIT));
        addMetricMeta("overview/ROWS_READ_PER_ROWS_RETURNED", new MetricMeta(false, DEFAULT_UNIT));
        addMetricMeta("overview/TOTAL_BP_HIT_RATIO_PERCENT", new MetricMeta(false, PERCENTAGE_UNIT));
       
        //Define connection overview metrics meta data
        addMetricMeta("connection_overview/connections", new MetricMeta(false, DEFAULT_UNIT));
        
        //Define current SQL overview metrics meta data
        addMetricMeta("sql_overview/SQL_statements", new MetricMeta(false, DEFAULT_UNIT));
        
    }

    /**
     * Add the given metric meta information to the Map of all metric meta information for this agent
     * 
     * @param String key
     * @param Metric mm
     */
    private void addMetricMeta(String key, MetricMeta mm) {
        metricsMeta.put(key.toLowerCase(), mm);
    }

    /**
     * Get meta of a give metrics
     * 
     * A default metric is a integer value
     * 
     * @param String Metric to look up
     * @return MetridMeta Structure of information about the metric
     */
    private MetricMeta getMetricMeta(String key) {
        return metricsMeta.get(key.toLowerCase()); // Look for existing meta data on metric
    }

    /**
     * Private utility function to validate that all required data is present for constructing atomic metrics
     * 
     * @param category - a display name for which metric category will not be included if a given key is not present
     * @param map - the map of available data points
     * @param keys - keys that are expected to be present for this operation
     * @return true if all expected keys are present, otherwise false
     */
    private boolean areRequiredMetricsPresent(String category, Map<String, Float> map, String... keys) {
        for (String key : keys) {
            if (!map.containsKey(key)) {
                if (firstReport) { // Only report missing category data on the first run so as not to clutter the log
                    logger.debug("Not reporting on '", category, "' due to missing data field '", key, "'");
                }

                return false;
            }
        }

        return true;
    }

    /**
     * Return the human readable name for this agent.
     * 
     * @return String
     */
    @Override
    public String getComponentHumanLabel() {
        return name;
    }

    /**
     * Return the map of metric categories
     * 
     * @return Map
     */
    public Map<String, Object> getMetricCategories() {
        return metricCategories;
    }
}
