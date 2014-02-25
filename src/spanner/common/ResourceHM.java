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

	protected static Logger logger = null;
	private HashMap<String, HashMap<String, String>> hbaseMap ;
	
	public ResourceHM(Logger logger)
	{
		hbaseMap = new HashMap<String, HashMap<String, String>>();
		this.logger = logger;
		
	}
	
	public ElementsSetProto ReadResource(ElementsSetProto readSet)
	{
		ElementsSetProto.Builder readResponse = ElementsSetProto.newBuilder();
		StringBuffer buffer = new StringBuffer();
		
		for(ElementProto element: readSet.getElementsList())
		{
			String rowKey = element.getRow();
			ElementProto.Builder recordBuilder = ElementProto.newBuilder().setRow(rowKey);
			HashMap<String, String> record = hbaseMap.get(rowKey);
			buffer.append("Reading Record "+rowKey+" :: ");
			for(ColElementProto colElem : element.getColsList())
			{
				String colName = colElem.getCol();
				buffer.append(colName+","+record.get(colName)+";");
				ColElementProto colElement = ColElementProto.newBuilder().setCol(colName).setValue(record.get(colName)).build();
				recordBuilder.addCols(colElement);
			}
			buffer.append("\n");
			readResponse.addElements(recordBuilder.build());
		}
		logger.log(Level.INFO, buffer.toString());
		return readResponse.build();
	}
	
	public boolean WriteResource(ElementsSetProto writeSet)
	{
		StringBuffer buffer = new StringBuffer();
		
		for(ElementProto element: writeSet.getElementsList())
		{
			String rowKey = element.getRow();
			buffer.append("Writing record "+rowKey+" :: ");
			HashMap<String, String> record = null;
			if(hbaseMap.containsKey(rowKey))
					record = hbaseMap.get(rowKey);
			else 
					record =  new HashMap<String, String>();
			
			for(ColElementProto colElem : element.getColsList())
			{
				record.put(colElem.getCol(), colElem.getValue());
				buffer.append(colElem.getCol()+","+colElem.getValue()+";");
			}
			hbaseMap.put(rowKey, record);
			buffer.append("\n");
		}
		logger.log(Level.INFO, buffer.toString());
		return true;
	}
	
}
