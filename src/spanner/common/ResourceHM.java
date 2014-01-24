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


public class ResourceHM {

	protected static Logger log = null;
	private HashMap<String, HashMap<String, String>> hbaseMap ;
	
	public ResourceHM(Logger logger)
	{
		hbaseMap = new HashMap<String, HashMap<String, String>>();
		log = logger;
		
	}
	
	
	
	/**
	 * Put (or insert) a row
	 */
	public HashMap<String, HashMap<String, String>> WriteResource(HashMap<String, HashMap<String, String>> data) throws Exception 
		{
			for(String rowKey: data.keySet())
			{
				if(hbaseMap.containsKey(rowKey))
				{
					HashMap<String, String> colMap = hbaseMap.get(rowKey);
					colMap.putAll(data.get(rowKey));
					hbaseMap.put(rowKey, colMap);
				}
				else{
					HashMap<String, String> colMap = new HashMap<String, String>();
					colMap.putAll(data.get(rowKey));
					hbaseMap.put(rowKey, colMap);
				}
			}
			return data;
	}
	
	/**
	 * Put (or insert) a row
	 */
	public void updateRecord(String rowKey, HashMap<String, String> colValues) throws Exception 
	{
		if(hbaseMap.containsKey(rowKey))
		{
			HashMap<String, String> colMap = hbaseMap.get(rowKey);
			colMap.putAll(colValues);
			hbaseMap.put(rowKey, colMap);
		}
		else{
			HashMap<String, String> colMap = new HashMap<String, String>();
			colMap.putAll(colValues);
			hbaseMap.put(rowKey, colMap);
		}
	}

	public ElementsSetProto ReadResource(ElementsSetProto readSet)
	{
		ElementsSetProto.Builder readResponse = ElementsSetProto.newBuilder();
		for(ElementProto element: readSet.getElementsList())
		{
			String rowKey = element.getRow();
			ElementProto.Builder recordBuilder = ElementProto.newBuilder().setRow(rowKey);
			HashMap<String, String> record = hbaseMap.get(rowKey);
			System.out.println("Record "+rowKey+" :: "+record);
			for(ColElementProto colElem : element.getColsList())
			{
				String colName = colElem.getCol();
				System.out.println("ColName "+colName+", value "+record.get(colName));
				ColElementProto colElement = ColElementProto.newBuilder().setCol(colName).setValue(record.get(colName)).build();
				recordBuilder.addCols(colElement);
			}
			readResponse.addElements(recordBuilder.build());
		}
		return readResponse.build();
	}
	
	public boolean WriteResource(ElementsSetProto readSet)
	{
		for(ElementProto element: readSet.getElementsList())
		{
			String rowKey = element.getRow();
			HashMap<String, String> record = null;
			if(hbaseMap.containsKey(rowKey))
					record = hbaseMap.get(rowKey);
			else 
					record =  new HashMap<String, String>();
			
			for(ColElementProto colElem : element.getColsList())
			{
				record.put(colElem.getCol(), colElem.getValue());
			}
			hbaseMap.put(rowKey, record);
		}
		return true;
	}
	
	public  HashMap<String, HashMap<String, String>> ReadResource(HashMap<String, ArrayList<String>> data) throws IOException{
		
		System.out.println("Trying to read ^^^^^^^^^^^^^^^^^^^^^^^^^ "+data);
		HashMap<String, HashMap<String, String>> result = new HashMap<String, HashMap<String, String>>();
		for(String rowKey: data.keySet())
		{
			HashMap<String, String> colValues = new HashMap<String, String>();
			ArrayList<String> colList = data.get(rowKey);
			if(hbaseMap.containsKey(rowKey))
			{
				
				HashMap<String, String> colMap = hbaseMap.get(rowKey);
				for(String colName: colList){
					if(colMap.containsKey(colName))
						colValues.put(colName, colMap.get(colName));
					else
						colValues.put(colName, null);
				}
			}
			else{
				System.out.println("Shouldn't reach this place ************* ");
			}
			result.put(rowKey, colValues);
		}
		return result;
	}
	
	
	/**
	 * Get a row
	 */
	public  HashMap<String, String> getOneRow (String rowKey) throws IOException{
		return hbaseMap.get(rowKey);
	}
	
	/**
	 * Get a row
	 */
	public  String getOneCol(String rowKey, String colName) throws IOException{
		
		return hbaseMap.get(rowKey).get(colName);
	}
	

}
