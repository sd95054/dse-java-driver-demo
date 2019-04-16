package com.datastax.dse.demo;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.github.javafaker.Faker;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class DriverQueryLoggingDemo {

    private static DseCluster cluster;
    private static DseSession session;
    private static QueryLogger queryLogger;
    private static Config conf = ConfigFactory.load();
    private static DCAwareRoundRobinPolicy dcAwareLBPolicy;
    private static Statement stmt;
    private static Logger logger;


    public static void main(String[] args) throws java.net.UnknownHostException, InterruptedException {
        System.out.println("Starting DriverQueryLoggingDemo...\n");

        logger = LogManager.getLogger(DriverQueryLoggingDemo.class);
        logger.setLevel(Level.ALL);

        connect();
        queryLoggerSetup();

        long statsDumpFreq = TimeUnit.SECONDS.toNanos(conf.getInt("DseCassandra.statsDumpFrequencySeconds"));
        int i = 1;
        long initial_time = System.nanoTime();

        // Loop and write a Table for a specified amount of time
        for (long stop=System.nanoTime()+TimeUnit.SECONDS.toNanos(conf.getInt("DseCassandra.writeSimulationTimeSeconds"));stop>System.nanoTime();) {
            writeTable();

            //If time exceeds a multiple of statsDumpFreq dump some metadata/statistics
            if (System.nanoTime() > (initial_time + statsDumpFreq*i)) {
                getMetaData();
                i++;
                Thread.sleep(conf.getInt("DseCassandra.threadSleepMilliseconds"));
            }
        }

        getMetaData();

        session.close();
        cluster.close();
    }


    public static void connect() throws java.net.UnknownHostException {

        List<String> ipaddresses = conf.getStringList("DseCassandra.hosts");


        ArrayList<InetAddress> hosts = new ArrayList<>();

        for (String x : ipaddresses) {
            hosts.add(InetAddress.getByName(x));
        }

        dcAwareLBPolicy = DCAwareRoundRobinPolicy.builder()
                .withLocalDc(conf.getString("DseCassandra.dcName"))
                .withUsedHostsPerRemoteDc(2)
                .allowRemoteDCsForLocalConsistencyLevel()
                .build();

        cluster = DseCluster.builder()
                .addContactPoints(hosts).withPort(conf.getInt("DseCassandra.PORT"))
                .withCredentials(conf.getString("DseCassandra.username").trim(), conf.getString("DseCassandra.password").trim())
                .withLoadBalancingPolicy(
                        dcAwareLBPolicy
                ).withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL.valueOf(conf.getString("DseCassandra.ConsistencyLevel"))))
                .build();

        session = cluster.connect();
    }

    public static void queryLoggerSetup() {

        queryLogger = QueryLogger.builder()
                .withConstantThreshold(1)
                .withMaxQueryStringLength(300)
                .withMaxParameterValueLength(200)
                .build();

        cluster.register(queryLogger);
    }


    public static void getMetaData() {

        Set<Host> hosts = cluster.getMetadata().getAllHosts();

        for (Host host : hosts ) {
            logger.info("Hostname: " + host.getAddress().toString());
            logger.info("DC Name: " + host.getDatacenter());
            logger.info("Rack Name: " + host.getRack());
            logger.info("State: " + host.getState());
        }

        Iterator<Host> queryPlanHosts = dcAwareLBPolicy.newQueryPlan("kroger_test", stmt);
        String hostlist = "";
        while (queryPlanHosts.hasNext() != false) {
            hostlist += queryPlanHosts.next().toString();
        }

        logger.info("query plan:" + hostlist);
    }

    public static void writeTable() {

        Faker faker = new Faker();
        String household = faker.numerify("##########");
        String div = faker.numerify("###");
        String store = faker.numerify("#####");
        String coupon_number = faker.numerify("#####");
        int data_groups = Integer.parseInt(faker.numerify("####"));
        long end_date = faker.date().past(180, TimeUnit.DAYS).getTime();
        double relevance_score = new Double(faker.numerify("##.####"));
        long start_date = faker.date().past(180, TimeUnit.DAYS).getTime();


         stmt = new SimpleStatement("INSERT INTO kroger_test.coupon_relevancy (household, div, store, coupon_number, data_groups, end_date, relevance_score, start_date) " +
                "VALUES (" +
                "'" + household + "'" + "," +
                "'" + div + "'" + "," +
                "'" + store + "'" + "," +
                "'" + coupon_number + "'" + "," +
                data_groups + "," +
                + end_date  + "," +
                relevance_score + "," +
                start_date +
                ")");

        ResultSet rs = session.execute(stmt);

        // Normal: rs.one() returns null
        // Need a check here to see if non null RS is returned

        if (rs.one() != null) {
            logger.error("ResultSet.one():" + rs.one().toString());
        }


    }

}
