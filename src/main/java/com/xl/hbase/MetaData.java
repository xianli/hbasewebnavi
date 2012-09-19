package com.xl.hbase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * hbase table meta information manager
 *
 */
public class MetaData {

	public static final String MODEL_FILE="/model.xml";
	
	private static final String N_COLUMN = "Column";
	private static final String N_TABLE = "Table";
	private static final String A_TABLENAME_T = "name";
	private static final String A_ROWKEY = "rowkey";
	
	private static Map<String, List<ColumnType>> tables = null;
	private static Map<String, String> rowkeys=null;
	
	static {
		loadTables();
	}
	
	public static  List<ColumnType> getTableMeta(String tableName) {
		return (tableName==null)?null:
			tables.get(tableName);
	}

	public static String[] getTableNames() {
		String[] s = new String[tables.keySet().size()];
		tables.keySet().toArray(s);
		return s;
	}
	
	public static String getRowKeyType(String tableName) {
		return rowkeys.get(tableName);
	}
	
	
	public static String getColumnType(String tableName, String fullColName) {
		List<ColumnType> allMeta = getTableMeta(tableName);
		Iterator<ColumnType> it = allMeta.iterator();
		while (it.hasNext()) {
			ColumnType cm = it.next();
			if ((cm.getFamilyName() + ":" + cm.getColName()).equals(fullColName)) {
				return cm.getType();
			}
		}
		return null;
	}
	
	private static void loadTables() {
		
		if (tables != null) return ;
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder builder = dbf.newDocumentBuilder();
			Document doc = builder.parse(new File(MetaData.class.getResource(MODEL_FILE).getFile()));
			Element elm = doc.getDocumentElement();
			NodeList nl = elm.getElementsByTagName(N_TABLE);
			tables = new HashMap<String,  List<ColumnType>>();
			rowkeys = new HashMap<String, String>();
			for (int i=0; i<nl.getLength(); i++) {
				List<ColumnType> columns = new ArrayList<ColumnType>();
				
				Element columnMeta = (Element)nl.item(i);
				String key = columnMeta.getAttribute(A_TABLENAME_T);
				String rowKeyType = columnMeta.getAttribute(A_ROWKEY);
				
				rowkeys.put(key, rowKeyType);
				//get its sub element: column
				NodeList nl2 = columnMeta.getElementsByTagName(N_COLUMN);
				for (int j=0; j<nl2.getLength(); j++) {
					Element col = (Element)nl2.item(j);
					ColumnType meta = new ColumnType();
					String name=col.getAttribute("name").trim(),
							type=col.getAttribute("type").trim().toLowerCase().replaceAll("\\s", ""),
							familyName=col.getAttribute("familyName").trim();
					if (isEmpty(name) || isEmpty(familyName)) {
						//invalid configuration, ignore this row
						Logger.getLogger(MetaData.class).warn("either column name " +
								"or family name is not specified!");
						continue;
					}
					meta.setColName(name);
					meta.setType(type);
					meta.setFamilyName(familyName);
					columns.add(meta);
				}
				if (columns.size() > 0) {
					tables.put(key, columns);
				}
			}
		} catch (ParserConfigurationException e) {
			Logger.getLogger(MetaData.class).error("ParserConfigurationException", e);
		} catch (SAXException e) {
			Logger.getLogger(MetaData.class).error("SAXException", e);
		} catch (IOException e) {
			Logger.getLogger(MetaData.class).error("IOException", e);
		}
	}
	
	private static boolean isEmpty(String length) {
		return length==null || length.equals("");
	}
	
	public static class ColumnType {
		private String colName;
		private String familyName;
		private String type;
		
		public String getColName() {
			return colName;
		}
		
		public void setColName(String colName) {
			this.colName = colName;
		}
		
		public String getFamilyName() {
			return familyName;
		}
		
		public void setFamilyName(String familyName) {
			this.familyName = familyName;
		}
		
		public String getFullColName() {
			return familyName+":"+colName;
		}
		
		public String getType() {
			return type;
		}
		
		public void setType(String type) {
			this.type = type;
		}
	}
}
