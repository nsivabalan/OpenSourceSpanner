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

import spanner.protos.Protos.ColElementProto;
import spanner.protos.Protos.ElementProto;
import spanner.protos.Protos.ElementsSetProto;


public class Resource {

	private static Configuration conf = null;
	protected static Logger logger = null;
	private final String familyName = "default_test";
	private final String qualifier = "default_qualifer";

	public Resource(Logger logger)
	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.master","localhost:9000");
		String[] familys = new String[]{familyName};
		this.logger = logger;
		//FIX ME: remove below line
		System.out.println("Logger "+logger+" ....");
		logger.log(Level.INFO, " test msg logging ...........................................................");
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
			logger.log(Level.INFO,"Table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(Common.tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			logger.log(Level.INFO,"Created table " + Common.tableName);
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

			}
			table.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}


	public boolean WriteResource(ElementsSetProto writeSet)
	{
		StringBuffer buffer = new StringBuffer();
		try {
		HTable table = new HTable(conf, Common.tableName);
		
		for(ElementProto element: writeSet.getElementsList())
		{
			String rowKey = element.getRow();
			buffer.append("Writing record "+rowKey+" :: ");
			Put put = new Put(Bytes.toBytes(rowKey));
			for(ColElementProto colElem : element.getColsList())
			{	
				put.add(Bytes.toBytes(familyName), Bytes.toBytes(colElem.getCol()), Bytes
						.toBytes(colElem.getValue()));
				buffer.append(colElem.getCol()+","+colElem.getValue()+";");
			}
			table.put(put);
			buffer.append("\n");
		}
		table.close();
		logger.log(Level.INFO, buffer.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
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
			logger.log(Level.INFO, "Udpated Record " + rowKey);
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
				}
			}
			result.put(rowKey, record);
		}
		table.close();
		return result;
	}


	public ElementsSetProto ReadResource(ElementsSetProto readSet)
	{
		ElementsSetProto.Builder readResponse = ElementsSetProto.newBuilder();
		StringBuffer buffer = new StringBuffer();

		try {
			HTable table = new HTable(conf, Common.tableName);

			for(ElementProto element: readSet.getElementsList())
			{
				String rowKey = element.getRow();
				Get get = new Get(rowKey.getBytes());
				get.addFamily(familyName.getBytes());
				ElementProto.Builder recordBuilder = ElementProto.newBuilder().setRow(rowKey);
				//HashMap<String, String> record = hbaseMap.get(rowKey);
				buffer.append("Reading Record "+rowKey+" :: ");
				Result rs = table.get(get);
				ArrayList<String> colList = new ArrayList<String>();
				for(ColElementProto colElem : element.getColsList())			
					colList.add(colElem.getCol());
				
				logger.log(Level.INFO, "List of cols "+colList);
				for(KeyValue kv : rs.raw()){
					
					String colName = new String(kv.getRow());
					String colVal = new String(kv.getValue());
					logger.log(Level.INFO, " :: "+colName+", "+colVal);
					if(colList.contains(colName)){
						logger.log(Level.INFO, "Found col "+colName+" with val "+colVal);
						ColElementProto colElement = ColElementProto.newBuilder().setCol(colName).setValue(colVal).build();
						recordBuilder.addCols(colElement);
						buffer.append(colName+","+colVal+";");
					}
				}

				buffer.append("\n");
				readResponse.addElements(recordBuilder.build());
			}
			logger.log(Level.INFO, buffer.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return readResponse.build();
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
			//System.out.print(new String(kv.getRow()) + " " );
			//System.out.println(new String(kv.getValue()));
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
			//System.out.print(new String(kv.getRow()) + " " );
			//System.out.println(new String(kv.getValue()));
		}
		table.close();
		return record.get(colName);
	}


}
