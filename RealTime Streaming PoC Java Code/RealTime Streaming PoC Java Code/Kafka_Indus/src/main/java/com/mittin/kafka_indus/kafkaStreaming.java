/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittin.kafka_indus;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author mittin
 */
public class kafkaStreaming {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        // TODO code application logic here
        
        BufferedReader br = null;
        
        Properties props = new Properties();
                 props.put("bootstrap.servers", "16.181.234.212:9092");
                 props.put("acks", "all");
                 props.put("retries", 0);
                 props.put("batch.size", 16384);
                 props.put("linger.ms", 1);
                 props.put("buffer.memory", 33554432);
                 props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
                 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                 
                 String topicName="indus_data_15";                
        
                 String msg="";
                 
                 Producer producer = new KafkaProducer<String, String>(props);

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader("C:\\Users\\mittin\\Documents\\Output_test1.csv"));

			while ((sCurrentLine = br.readLine()) != null) {				                                
                            msg=sCurrentLine;
                                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, msg); 
                                producer.send(rec);
                                System.out.println(sCurrentLine);
                                Thread.sleep(1000);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
    }
    
}
