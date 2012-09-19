package com.xl.hbase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.xl.hbase.MetaData.ColumnType;

/**
 * read data from hbase table
 */
public class Reader {

	private static final int INSTANCE_NUM = 10;
	private static final String CONFIG_FILE="/conf.properties";
	private static Logger LOG = Logger.getLogger(Reader.class);
	private static HTablePool pool;
	private static Configuration hbaseconf;
	private static Properties props;
	private static Interpreter intp;
	
	static {
		init();
		loadInterpreter();
	}
	
	private static void init() {
		//initialize htable pool;
		try {
			props = new Properties();
			props.load(new FileReader(new File(
					Reader.class.getResource(CONFIG_FILE).getFile())));
			hbaseconf = HBaseConfiguration.create();
			// set all hbase properties
			Iterator<?> it = props.keySet().iterator();
			while (it.hasNext()) {
				String key = (String)it.next();
				if (key.startsWith("hbase.")) {
					hbaseconf.set((String)key,(String)props.get(key));
				}
			}
			pool = new HTablePool(hbaseconf, INSTANCE_NUM);
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
		}
	}
	
	
	private static void loadInterpreter() {
		try {
			String className = props.getProperty("interpreter.class");
			if ("".equals(className)) 
				className = "com.xl.hbase.InterpreterImpl";
			Class<?> clazz = Class.forName(className);
			intp =  (Interpreter)clazz.newInstance();
		} catch (ClassNotFoundException e) {
			LOG.error("class not found", e);
		} catch (InstantiationException e) {
			LOG.error("instantiation exception", e);
		} catch (IllegalAccessException e) {
			LOG.error("illegal access", e);
		}
	}
	
	private String tableName;
	
	public Reader(String tableName) {
		this.tableName = tableName;
	}

	public JSONArray get(byte[] rowkey) {
		List<ColumnType> list = MetaData.getTableMeta(tableName);
		if (list==null) {
			LOG.error(String.format("tablename %s not found in table configration!", 
					tableName));
			return null;
		}
		HTable table = null;
		try {
			long startT = System.currentTimeMillis();
			Get get = new Get(rowkey);
			
			table = (HTable)pool.getTable(tableName);
			Result result = table.get(get);
			long time = (System.currentTimeMillis() - startT) ;
			if (result.isEmpty()) {
				LOG.info(String.format("query %s on %s takes %s ms, get 0 rows", 
						Util.bytes2String(rowkey, MetaData.getRowKeyType(tableName)),
						tableName, time ));
				return null;
			} else {
				LOG.info(String.format("query %s on %s takes %s ms, get 1 rows", 
						Util.bytes2String(rowkey, MetaData.getRowKeyType(tableName)),
						tableName, time ));
			}
			JSONArray ret = new JSONArray();
			ret.put(toJSON(result));
			return ret;
		} catch (IOException e) {
			LOG.error("hbase query error", e);
			return null;
		} finally {
			if (table!=null)
				try {
					table.close();
				} catch (IOException e) {
					LOG.error("htable close error", e);
				}
		}
	}
	
	public JSONArray get(List<byte[]> rowkeys) {
		List<ColumnType> list = MetaData.getTableMeta(tableName);
		if (list==null) {
			LOG.error(String.format("tablename %s not found in table configration!", 
					tableName));
			return null;
		}
		HTable table = null;
		try {
			long start = System.currentTimeMillis();
			List<Get> gets = new ArrayList<Get>();
			for (int i=0;i<rowkeys.size();++i) {
				gets.add(new Get(rowkeys.get(i)));
			}
			
			table = (HTable)pool.getTable(tableName);
			Result[] result = table.get(gets);
			long time = (System.currentTimeMillis() - start) ;
			LOG.info(String.format(" query %s on %s takes %s ms, get 0 rows", 
					Util.bytes2String(rowkeys, MetaData.getRowKeyType(tableName)),
					tableName, time ));
			
			JSONArray ret = new JSONArray();
			for (int i=0;i<result.length;i++) {
				if (!result[i].isEmpty()) {
					ret.put(toJSON(result[i]));
				}
			}
			return ret;
		} catch (IOException e) {
			LOG.error("hbase query error", e);
			return null;
		} finally {
			if (table!=null)
				try {
					table.close();
				} catch (IOException e) {
					LOG.error("htable close error", e);
				}
		}
	}
	
	
	/**
	 * get 'rowNum' rows, the method will return null if scan takes more than
	 * defined time 
	 * @param rowNum
	 * @param totalCnt  set it to null by default. 
	 * @return
	 */
	public JSONArray get(int rowNum) {
		HTable table = null;
		ResultScanner rs = null;
		try {
			table = (HTable)pool.getTable(tableName);
			Scan scan = new Scan();
			scan.setFilter(new RandomRowFilter(0.5f));
			long start = System.currentTimeMillis();
			rs = table.getScanner(scan);
			Iterator<Result> ir = rs.iterator();
			JSONArray ret = new JSONArray();
			while (ir.hasNext()) {
				ret.put(toJSON(ir.next()));
				if (ret.length() >= rowNum) break;
			}
		
			LOG.info(String.format("scan the table %s takes %s ms, get %s rows", 
					new String(table.getTableName()),
					System.currentTimeMillis()-start,
					ret.length()));
			return ret;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			return null;
		} finally {
			if (rs!=null) rs.close();
			if (table!=null)
				try {
					table.close();
				} catch (IOException e) {
					LOG.error("table close error", e);
				}
		}
	}
	
	public JSONArray scan(byte[] startkey, byte[] endkey) {
		HTable table = null;
		ResultScanner rs = null;
		try {
			long startT = System.currentTimeMillis();
			Scan scan = new Scan(startkey, endkey);
			
			table = (HTable)pool.getTable(tableName);
			rs = table.getScanner(scan);
			
			//get max return 
			int maxRowNum = Util.parseInt(props.getProperty("return.max"), 1000);
			JSONArray ret = new JSONArray();
			for (Result res : rs) {
				ret.put(toJSON(res));
				if (ret.length() >= maxRowNum)
					break;
			}
			LOG.info(String.format("scan on %s by start key: %s, end key: %s takes %s ms, get %s rows", 
					new String(table.getTableName()), 
					Util.bytes2String(startkey, MetaData.getRowKeyType(tableName)),
					Util.bytes2String(endkey, MetaData.getRowKeyType(tableName)),
					(System.currentTimeMillis() - startT),
					ret.length()));
			return ret;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			return null;
		} finally {
			if (rs!=null) rs.close();
			if (table!=null)
				try {
					table.close();
				} catch (IOException e) {
					LOG.error("table close error", e);
				}
		}
	}
	
	public int count() {
		HTable table = null;
		ResultScanner rs = null ;
		try {
			//default 2 minutes
			int timeout = Util.parseInt("return.timeout", 2) ; 
			long start=System.currentTimeMillis();
			table = (HTable)pool.getTable(tableName);
			Scan scan = new Scan();
			rs = table.getScanner(scan);
			
			JSONArray ret = new JSONArray();
			for (Result res : rs) {
				ret.put(toJSON(res));
				if (System.currentTimeMillis()-start > timeout*60*1000) {
					LOG.warn("scan exceeds " + timeout);
					break;
				}
			}
			LOG.info(String.format("count the table %s takes %s ms", 
					new String(table.getTableName()),
					System.currentTimeMillis()-start ));
			return ret.length();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			return -1;
		} finally {
			if (rs!=null) rs.close();
			if (table!=null)
				try {
					table.close();
				} catch (IOException e) {
					LOG.error("table close error", e);
				}
		}
	}
	
	public JSONObject toJSON(Result result) {
		Iterator<KeyValue> kvs = result.list().iterator();
		JSONObject row = new JSONObject();
		String keyStr = null;
		try {
			JSONObject column = new JSONObject();
			while (kvs.hasNext()) {
				KeyValue kv = kvs.next();
				if (keyStr == null)
					keyStr = intp.interpretRowkey(tableName, kv.getRow());
				byte[] cf = kv.getFamily();
				byte[] col = kv.getQualifier();
				long ts = kv.getTimestamp();
				String columnName = Bytes.toString(cf)+":"+
				Bytes.toString(col)+","+ts;
				column.put(columnName, intp.interpretColumnValue
						(tableName, kv.getKey(), cf, col, ts, kv.getValue()));
			}
			row.put(keyStr, column);
		} catch (JSONException e) {
			LOG.error("", e);
		}
		return row;
	}
}