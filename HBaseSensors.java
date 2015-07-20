package sensors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
 
public class HBaseSensors {
 
    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }
 
    /**
     * Create a table
     */
    @SuppressWarnings("deprecation")
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
        admin.close();
    }
 
    /**
     * Delete a table
     */
    @SuppressWarnings("deprecation")
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Put (or insert) a row
     */
    @SuppressWarnings("deprecation")
    public static void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Delete a row
     */
    @SuppressWarnings("deprecation")
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
        table.close();
    }
 
    /**
     * Get a row
     */
    @SuppressWarnings("deprecation")
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
        table.close();
    }
    /**
     * Scan (or list) a table
     */
    @SuppressWarnings("deprecation")
    public static void getAllRecord (String tableName) {
        try{
             HTable table = new HTable(conf, tableName);
             Scan s = new Scan();
             ResultScanner ss = table.getScanner(s);
             for(Result r:ss){
                 for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                 }
             }
             table.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    
    /**
     * return average
     * @param tableName,range, and data
     */
    public static void dayAverage (String tableName){
        
    }
 
    public static void main(String[] agrs) {
        try {
            
            
            // Create table "sensor-1";
            String tableSensor1 = "sensor-1";
            String[] sensor1Family = {"sensorData1"};
            HBaseSensors.creatTable(tableSensor1, sensor1Family);
            
            // Create table "sensor-2";
            String tableSensor2 = "sensor-2";
            String[] sensor2Family = {"sensorData2"};
            HBaseSensors.creatTable(tableSensor2, sensor2Family);
            
            
            int row = 0;
            int temperature = 0;
            int humidity = 0;
            int light = 0;
            String nutrition = "10:20:30";
            
            Random generator = new Random();
            
            HBaseSensors.addRecord(tableSensor1, "1", "sensorData1", "time", "201507010100am");
            HBaseSensors.addRecord(tableSensor1, "2", "sensorData1", "time", "201507010300am");
            HBaseSensors.addRecord(tableSensor1, "3", "sensorData1", "time", "201507010500am");
            HBaseSensors.addRecord(tableSensor1, "4", "sensorData1", "time", "201507010700am");
            HBaseSensors.addRecord(tableSensor1, "5", "sensorData1", "time", "201507010900am");
            HBaseSensors.addRecord(tableSensor1, "6", "sensorData1", "time", "201507011100am");
            HBaseSensors.addRecord(tableSensor1, "7", "sensorData1", "time", "201507010100pm");
            HBaseSensors.addRecord(tableSensor1, "8", "sensorData1", "time", "201507010300pm");
            HBaseSensors.addRecord(tableSensor1, "9", "sensorData1", "time", "201507010500pm");
            HBaseSensors.addRecord(tableSensor1, "10", "sensorData1", "time", "201507010700pm");
            HBaseSensors.addRecord(tableSensor1, "11", "sensorData1", "time", "201507010900pm");
            HBaseSensors.addRecord(tableSensor1, "12", "sensorData1", "time", "201507011100pm,");
            
            for (int z = 0; z < 12; z++)
            {
                row++;    
                temperature = 50 + generator.nextInt(30);
                humidity = 50 + generator.nextInt(30);
                light = 50 + generator.nextInt(30);
                
                HBaseSensors.addRecord(tableSensor1, Integer.toString(row), "sensorData1", "temperature", Integer.toString(temperature));
                HBaseSensors.addRecord(tableSensor1, Integer.toString(row), "sensorData1", "humidity", Integer.toString(humidity));
                HBaseSensors.addRecord(tableSensor1, Integer.toString(row), "sensorData1", "light", Integer.toString(light));
                HBaseSensors.addRecord(tableSensor1, Integer.toString(row), "sensorData1", "nutrition", nutrition);
            }          
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
