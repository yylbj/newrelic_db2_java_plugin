package com.newrelic.plugins.db2.instance;

import com.newrelic.metrics.publish.Runner;
import com.newrelic.metrics.publish.configuration.ConfigurationException;

/**
 * This is the main calling class for a New Relic Agent. This class sets up the
 * necessary agents from the provided configuration and runs these indefinitely.
 * 
 * @author yylbj@cn.ibm.com
 * 
 */
public class Main {
    
    public static void main(String[] args) {
        try {
            Runner runner = new Runner();
            runner.add(new DB2AgentFactory());
            runner.setupAndRun(); // Never returns
        } catch (ConfigurationException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
