/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittin.storm_indus;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.trees.DecisionStump;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

/**
 *
 * @author mittin
 */
public class StormBolt implements IBasicBolt{


 Connection conn;
  Statement stmt;
  
    public void prepare(Map stormConf, TopologyContext topologyContext) {
        Properties myProp = new Properties();
        myProp.put("user", "dbadmin");
        myProp.put("password", "andmpocs@123");		        
	 
        try {
            conn = DriverManager.getConnection(
                            "jdbc:vertica://16.181.234.210:5433/ANDMPOCS",
                            myProp);
            // establish connection and make a table for the data.
            stmt = conn.createStatement();
        }
        catch(SQLException  e){
            e.printStackTrace();
        }
    }
    
    public void execute(Tuple input, BasicOutputCollector collector) {
        String fields;
     fields = input.getString(0);
     
     String header="Site_Key,MAINS_FAIL_Cleared_Count,MAINS_FAIL_Cleared_Duration,MAINS_FAIL_Uncleared_Count,MAINS_FAIL_Uncleared_Duration,SOB_Cleared_Count,SOB_Cleared_Duration,SOB_Uncleared_Count,SOB_Uncleared_Duration,LBV_Cleared_Count,LBV_Cleared_Duration,LBV_Uncleared_Count,LBV_Uncleared_Duration,HRT_Cleared_Count,HRT_Cleared_Duration,HRT_Uncleared_Count,HRT_Uncleared_Duration,DGON_Cleared_Count,DGON_Cleared_Duration,DGON_Uncleared_Count,DGON_Uncleared_Duration,DOOR_OPEN_Cleared_Count,DOOR_OPEN_Cleared_Duration,DOOR_OPEN_Uncleared_Count,DOOR_OPEN_Uncleared_Duration,FIRE_AND_SMOKE_Cleared_Count,FIRE_AND_SMOKE_Cleared_Duration,FIRE_AND_SMOKE_Uncleared_Count,FIRE_AND_SMOKE_Uncleared_Duration,DG_MAJOR_FAULT_Cleared_Count,DG_MAJOR_FAULT_Cleared_Duration,DG_MAJOR_FAULT_Uncleared_Count,DG_MAJOR_FAULT_Uncleared_Duration,DG_FUEL_LEVEL_LOW_Cleared_Count,DG_FUEL_LEVEL_LOW_Cleared_Duration,DG_FUEL_LEVEL_LOW_Uncleared_Count,DG_FUEL_LEVEL_LOW_Uncleared_Duration,FCU_ON_Cleared_Count,FCU_ON_Cleared_Duration,FCU_ON_Uncleared_Count,FCU_ON_Uncleared_Duration,FCU_FAIL_Cleared_Count,FCU_FAIL_Cleared_Duration,FCU_FAIL_Uncleared_Count,FCU_FAIL_Uncleared_Duration,DC_FUSE_FAIL_Cleared_Count,DC_FUSE_FAIL_Cleared_Duration,DC_FUSE_FAIL_Uncleared_Count,DC_FUSE_FAIL_Uncleared_Duration,AC_POWER_FAIL_Cleared_Count,AC_POWER_FAIL_Cleared_Duration,AC_POWER_FAIL_Uncleared_Count,AC_POWER_FAIL_Uncleared_Duration,RECTIFIER_FAIL_Cleared_Count,RECTIFIER_FAIL_Cleared_Duration,RECTIFIER_FAIL_Uncleared_Count,RECTIFIER_FAIL_Uncleared_Duration,SECTOR_DOWN_Cleared_Count,SECTOR_DOWN_Cleared_Duration,SECTOR_DOWN_Uncleared_Count,SECTOR_DOWN_Uncleared_Duration,BSC_DOWN_Cleared_Count,BSC_DOWN_Cleared_Duration,BSC_DOWN_Uncleared_Count,BSC_DOWN_Uncleared_Duration,PIU_MANUAL_Cleared_Count,PIU_MANUAL_Cleared_Duration,PIU_MANUAL_Uncleared_Count,PIU_MANUAL_Uncleared_Duration,THEFT_Cleared_Count,THEFT_Cleared_Duration,THEFT_Uncleared_Count,THEFT_Uncleared_Duration,site_down\n";
        
        String data=fields;
        
       
        
        File file = new File("streamdata.csv");
        try {
        file.createNewFile();
        
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(header);
        bw.write(data);
        bw.close();
        
        
        DecisionStump ds = (DecisionStump) weka.core.SerializationHelper.read("DecisionStump.model");
        
        CSVLoader loader = new CSVLoader();
//       String[] options = new String[1]; 
//       options[0] = "-H";
       
       loader.setSource(new File("C:\\Users\\kanike\\Documents\\NetBeansProjects\\Storm_Indus\\streamdata.csv"));
       loader.setOptions(new String[] {"-H"});
       
       
       Instances testDataset = loader.getDataSet();
       testDataset.setClassIndex(testDataset.numAttributes()-1); 
       
       double label = ds.classifyInstance(testDataset.instance(0));
       
       
       testDataset.instance(0).setClassValue(label);
        

       System.out.println(testDataset.instance(0).toString());
       
       String[] parts;

      
       parts = testDataset.instance(0).toString().split(",");              
       
       
        
       float flag = Float.parseFloat(parts[73]);
       
       String query="";
       
       if (flag > 0.8){       
       query="Insert into RTDSP.Indus_Streaming_data values("+parts[0]+","+"'"+"No Outage"+"'"+")";
       }
       else{
           query="Insert into RTDSP.Indus_Streaming_data values("+parts[0]+","+"'"+"Outage"+"'"+")";
       }
       
       
       stmt.execute(query);                
               
       }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
 
 public void cleanup() {
     try {
         conn.close();
     } catch (SQLException ex) {
         Logger.getLogger(StormBolt.class.getName()).log(Level.SEVERE, null, ex);
     }
	}
}
