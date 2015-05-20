package com.newrelic.plugins.db2.instance;

import static com.newrelic.plugins.db2.util.Constants.COMMA;
import static com.newrelic.plugins.db2.util.Constants.EMPTY_STRING;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.AgentFactory;
import com.newrelic.metrics.publish.configuration.ConfigurationException;
import com.newrelic.metrics.publish.util.Logger;

/**
 * This class produces the necessary Agents to perform gathering and reporting
 * metrics for the DB2 plugin
 * 
 * @author yylbj@cn.ibm.com
 * 
 */
public class DB2AgentFactory extends AgentFactory {

    private static final String CATEGORY_CONFIG_FILE = "metric.category.json";
    private static final Logger logger = Logger.getLogger(DB2Agent.class);
    
    /**
     * Configure an agent based on an entry in the properties file. There may be
     * multiple agents per plugin
     */
    @Override
    public Agent createConfiguredAgent(Map<String, Object> properties) throws ConfigurationException {
        String name = (String) properties.get("name");
        String host = (String) properties.get("host");
        String database = (String) properties.get("database");
        String user = (String) properties.get("user");
        String passwd = (String) properties.get("passwd");
        String conn_properties = (String) properties.get("properties");
        String metrics = (String) properties.get("metrics");

        if (name == null || EMPTY_STRING.equals(name)) {
            throw new ConfigurationException("The 'name' attribute is required. Have you configured the 'config/plugin.json' file?");
        }
        if (database == null || EMPTY_STRING.equals(database)) {
            throw new ConfigurationException("The 'database' attribute is required. Have you configured the 'config/plugin.json' file?");
        }
        if (user == null || EMPTY_STRING.equals(user)) {
            throw new ConfigurationException("The 'user' attribute is required. Have you configured the 'config/plugin.json' file?");
        }
        if (passwd == null || EMPTY_STRING.equals(passwd)) {
            throw new ConfigurationException("The 'passwd' attribute is required. Have you configured the 'config/plugin.json' file?");
        }
        
        /**
         * Use pre-defined defaults to simplify configuration
         */
        if (host == null || EMPTY_STRING.equals(host)) {
            host = DB2Agent.AGENT_DEFAULT_HOST;
        }
        if (metrics == null || EMPTY_STRING.equals(metrics)) {
            metrics = DB2Agent.AGENT_DEFAULT_METRICS;
        }

        return new DB2Agent(name, host,database, user, passwd, conn_properties,
                processMetricCategories(metrics), readCategoryConfiguration());
    }

    /**
     * Read metric category information that enables the dynamic definition of
     * DB2 metrics that can be collected.
     * 
     * @return Map Categories and the meta data about the categories
     * @throws ConfigurationException
     */
    public Map<String, Object> readCategoryConfiguration() throws ConfigurationException {
        Map<String, Object> metricCategories = new HashMap<String, Object>();
        try {
            JSONArray json = readJSONFile(CATEGORY_CONFIG_FILE);
            for (int i = 0; i < json.size(); i++) {
                JSONObject obj = (JSONObject) json.get(i);
                String category = (String) obj.get("category");
                metricCategories.put(category.toLowerCase(), obj);
            }
        } catch (ConfigurationException e) {
            throw new ConfigurationException("'metric_categories' could not be found in the 'plugin.json' configuration file");
        }
        return metricCategories;
    }

    Set<String> processMetricCategories(String metrics) {
        String[] categories = metrics.toLowerCase().split(COMMA);
        Set<String> set = new HashSet<String>(Arrays.asList(categories));
        set.remove(EMPTY_STRING); // in case of trailing comma or two consecutive commas
        logger.debug("Metrics category to report are:", set);
        return set;
    }
}
