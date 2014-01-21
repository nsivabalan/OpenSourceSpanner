package spanner.common;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class Resource {

	private static Configuration conf = null;
	protected static Logger log = null;
	private final String familyName = "default_test";
	private final String qualifier = "default_qualifer";
	
	public Resource(Logger logger)
	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.master","localhost:9000");
		String[] familys = new String[]{familyName};
		log = logger;
		try {
			this.createTable(familys);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Create a table
	 */
	public void createTable(String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(Common.tableName)) {
			log.log(Level.INFO,"table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(Common.tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			log.log(Level.INFO,"create table " + Common.tableName + " ok.");
			admin.close();
		}
	}
	
	/**
	 * Put (or insert) a row
	 */
	public HashMap<String, HashMap<String, String>> WriteResource(HashMap<String, HashMap<String, String>> data) throws Exception {
		try {

			HTable table = new HTable(conf, Common.tableName);
			for(String rowKey : data.keySet()){
			Put put = new Put(Bytes.toBytes(rowKey));
			HashMap<String, String> colValues = data.get(rowKey);
			for(String colName : colValues.keySet())
			put.add(Bytes.toBytes(familyName), Bytes.toBytes(colName), Bytes
					.toBytes(colValues.get(colName)));
			table.put(put);
			System.out.println("udpated recored " + rowKey + " to table "
					+ Common.tableName + " ok.");
			}
			table.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}
	
	/**
	 * Put (or insert) a row
	 */
	public void updateRecord(String rowKey, HashMap<String, String> colValues) throws Exception {
		try {

			HTable table = new HTable(conf, Common.tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			for(String key: colValues.keySet())
			put.add(Bytes.toBytes(familyName), Bytes.toBytes(key), Bytes
					.toBytes(colValues.get(key)));
			table.put(put);
			
			System.out.println("udpated recored " + rowKey + " to table "
					+ Common.tableName + " ok.");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	
	public  HashMap<String, HashMap<String, String>> ReadResource(HashMap<String, ArrayList<String>> data) throws IOException{
		
		HTable table = new HTable(conf, Common.tableName);
		HashMap<String, HashMap<String, String>> result = new HashMap<String, HashMap<String, String>>();
		for(String rowKey : data.keySet()){
		Get get = new Get(rowKey.getBytes());
		
		ArrayList<String> colList = data.get(rowKey);
		
		get.addFamily(familyName.getBytes());
		
		Result rs = table.get(get);
		HashMap<String, String> record = new HashMap<String, String>();
		for(KeyValue kv : rs.raw()){
			String colName = new String(kv.getRow());
			String colVal = new String(kv.getValue());
			if(colList.contains(colName)){
			record.put(colName, colVal);
			System.out.print(new String(kv.getRow()) + " " );
			System.out.println(new String(kv.getValue()));
			}
		}
		result.put(rowKey, record);
		}
		table.close();
		return result;
	}
	
	
	/**
	 * Get a row
	 */
	public  HashMap<String, String> getOneRow (String rowKey) throws IOException{
		Get get = new Get(rowKey.getBytes());
		get.addFamily(familyName.getBytes());
		HTable table = new HTable(conf, Common.tableName);
		Result rs = table.get(get);
		HashMap<String, String> record = new HashMap<String, String>();
		for(KeyValue kv : rs.raw()){
			
			record.put(new String(kv.getRow()), new String(kv.getValue()));
			System.out.print(new String(kv.getRow()) + " " );
			System.out.println(new String(kv.getValue()));
		}
		table.close();
		return record;
	}
	
	/**
	 * Get a row
	 */
	public  String getOneCol(String rowKey, String colName) throws IOException{
		Get get = new Get(rowKey.getBytes());
		get.addColumn(familyName.getBytes(), qualifier.getBytes());
		HTable table = new HTable(conf, Common.tableName);
		Result rs = table.get(get);
		HashMap<String, String> record = new HashMap<String, String>();
		for(KeyValue kv : rs.raw()){
			
			record.put(new String(kv.getRow()), new String(kv.getValue()));
			System.out.print(new String(kv.getRow()) + " " );
			System.out.println(new String(kv.getValue()));
		}
		table.close();
		return record.get(colName);
	}
	

}
