/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittin.storm_indus;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 *
 * @author mittin
 */
public class KafkaSpout {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        // TODO code application logic here
        
        ZkHosts zkHosts=new ZkHosts("16.181.234.212:2181");
         
        String topic_name="indus_data_14";
        String consumer_group_id="test-consumer-group";
        String zookeeper_root="";
        SpoutConfig kafkaConfig=new SpoutConfig(zkHosts, topic_name, zookeeper_root, consumer_group_id);
         
        kafkaConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart=true;
         
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("KafkaSpout", new storm.kafka.KafkaSpout(kafkaConfig), 1);
        builder.setBolt("StormBolt", new StormBolt()).globalGrouping("KafkaSpout");
         
        Config config=new Config();
         
        LocalCluster cluster=new LocalCluster();
         
        cluster.submitTopology("KafkaConsumerTopology", config, builder.createTopology());
         
        try{
         for (int i = 0; i < 100; i++) {
        Thread.sleep(30 * 1000);
         }
        }catch(InterruptedException ex)
        {
         ex.printStackTrace();
        }
         
        cluster.killTopology("KafkaConsumerTopology");
        cluster.shutdown();
    }
    
}
