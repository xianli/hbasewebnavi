package com.xl.hbase;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * an interpreter that leverage the column data type information in model.xml
 * to interpret the row key and column value.
 */
public class InterpreterImpl implements Interpreter {

	enum DataType {INT, LONG, DOUBLE, BYTE, STRING, BYTES}
	@Override
	public String interpretRowkey(String tableName, byte[] rowkey) {
		String type = MetaData.getRowKeyType(tableName);
		String[] types = type.split("\\,");
		byte[] v = rowkey;
		StringBuilder builder = new StringBuilder();
		for (int j=0, start=0;j<types.length;j++) {
			if (types[j].equalsIgnoreCase(DataType.INT.toString())) {
				builder.append(Bytes.toInt(Arrays.copyOfRange(v, start, start+4)))
						.append(",");
				start+=4;
			} else if (types[j].equalsIgnoreCase(DataType.LONG.toString())) {
				builder.append(Bytes.toLong(Arrays.copyOfRange(v, start, start+8)))
						.append(",");
				start+=8;
			} else if (types[j].equalsIgnoreCase(DataType.DOUBLE.toString())) {
				builder.append(Bytes.toDouble(Arrays.copyOfRange(v, start, start+8)))
				.append(",");
				start+=8;
			} else if (types[j].equalsIgnoreCase(DataType.BYTE.toString())) {
				Arrays.copyOfRange(v, start, start+1);
				start+=1;
			} else if (types[j].equalsIgnoreCase(DataType.STRING.toString())) {
				int len  = Bytes.toInt(Arrays.copyOfRange(v, start, start+4));
				start += 4;
				Arrays.copyOfRange(v, start, start+len);
				start+=len;
			} 
		}
		if (builder.length()>1) 
			builder.delete(builder.length()-1, builder.length());
		return builder.toString();
	}

	@Override
	public String interpretColumnValue(String tableName, byte[] rowkey, byte[] cf,
						byte[] column, long timestamp, byte[] value) {
		String type = MetaData.getColumnType(tableName, 
				Bytes.toString(cf)+":"+Bytes.toString(column));
		if (type==null) return ""; 
		String[] types = type.split("\\,");
		//the first 4 bytes is count;
		int count = Bytes.toInt(Arrays.copyOfRange(value, 0, 4));
		
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		
		for (int i=0, start=4;i<count;i++) {
			for (int j=0;j<types.length;j++) {
				if (types[j].equalsIgnoreCase(DataType.INT.toString())) {
					builder.append(Bytes.toInt(Arrays.copyOfRange(value, start, start+4)))
							.append(",");
					start+=4;
				} else if (types[j].equalsIgnoreCase(DataType.LONG.toString())) {
					builder.append(Bytes.toLong(Arrays.copyOfRange(value, start, start+8)))
							.append(",");
					start+=8;
				} else if (types[j].equalsIgnoreCase(DataType.DOUBLE.toString())) {
					builder.append(Bytes.toDouble(Arrays.copyOfRange(value, start, start+8)))
					.append(",");
					start+=8;
				} else if (types[j].equalsIgnoreCase(DataType.BYTE.toString())) {
					Arrays.copyOfRange(value, start, start+1);
					start+=1;
				} else if (types[j].equalsIgnoreCase(DataType.STRING.toString())) {
					int len  = Bytes.toInt(Arrays.copyOfRange(value, start, start+4));
					start += 4;
					Arrays.copyOfRange(value, start, start+len);
					start+=len;
				} 
			}
		}
		if (builder.length()>1) 
			builder.delete(builder.length()-1, builder.length());
		builder.append("]");
		return builder.toString();
	}
}
