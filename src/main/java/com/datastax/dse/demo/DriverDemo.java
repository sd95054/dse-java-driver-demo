package com.datastax.dse.demo;

import com.datastax.driver.core.*;
import com.datastax.driver.dse.*;   //DSE Driver variant
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.InetAddress;
import java.util.ArrayList;

import java.util.List;


public class DriverDemo {

    private static DseCluster cluster;  //Change DseCluster to Cluster for OSS variant
    private static DseSession session;  //Change DseSession to Session for OSS variant

    public static void main(String[] args) throws java.net.UnknownHostException {
        System.out.println("Starting DriverDemo...\n");

        connect();
        driverDetails();

        session.close();
        cluster.close();
    }


    public static void connect() throws java.net.UnknownHostException {
        Config conf = ConfigFactory.load();
        List<String> ipaddresses = conf.getStringList("DseCassandra.hosts");


        ArrayList<InetAddress> hosts = new ArrayList<>();

        for (String x : ipaddresses) {
            hosts.add(InetAddress.getByName(x));
        }

        //Use Cluster.builder() for OSS and DseCluster.builder() for DSE
        cluster = DseCluster.builder()
                .addContactPoints(hosts).withPort(conf.getInt("DseCassandra.PORT"))
                .withCredentials(conf.getString("DseCassandra.username").trim(), conf.getString("DseCassandra.password").trim())
                .withLoadBalancingPolicy(
                        DCAwareRoundRobinPolicy.builder()
                                .withLocalDc(conf.getString("DseCassandra.dcName"))
                                .build()
                ).withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL.valueOf(conf.getString("DseCassandra.ConsistencyLevel"))))
                .build();

        session = cluster.connect();
    }

    public static void driverDetails(){
        System.out.println("Driver Details...\n");
        System.out.println("Driver version:" + cluster.getDriverVersion());
        System.out.println("Cluster type:" + cluster.getClass().getSimpleName());

    }


}
