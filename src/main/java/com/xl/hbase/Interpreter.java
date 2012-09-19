package com.xl.hbase;

/**
 * Interpreter is called by Reader to interpret the row key and column value in KeyValue object
 */
public interface Interpreter {
	/**
	 * 
	 * @param tableName
	 * @param rowkey
	 * @return  string format of row key
	 */
	String interpretRowkey(String tableName, byte[] rowkey);
	
	/**
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param cf
	 * @param column
	 * @param timestamp
	 * @param value 
	 * @return string format of column value
	 */
	String interpretColumnValue(String tableName, byte[] rowkey, 
			byte[] cf, byte[] column, long timestamp, 
			byte[] value);
}
