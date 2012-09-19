package com.xl.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * create test data
 * @author jiangxl
 *
 */
public class DataFactory {
	
	private final Logger LOG = Logger.getLogger(DataFactory.class);
	private static final String HOST="vmdev40";
	
	Configuration conf ;
	
	public DataFactory() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", HOST);
	}
	
	public boolean createTable(final String tableName,
			final String defaultColFam) {
		return createTable(tableName, defaultColFam, 1);
	}

	public boolean createTable(final String tableName,
			final String defaultColFam, final int maxVersion) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				LOG.error("cannot create table as table already exists");
				return false;
			}
			HTableDescriptor dscp = new HTableDescriptor(tableName);
			HColumnDescriptor cfdscp = new HColumnDescriptor(defaultColFam);
			cfdscp.setMaxVersions(maxVersion);
			dscp.addFamily(cfdscp);
			admin.createTable(dscp);
			return (admin.tableExists(tableName));
		} catch (MasterNotRunningException e) {
			LOG.error(e.getMessage(), e);
		} catch (ZooKeeperConnectionException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	public void artificalData(String tableName) {
		try {
			HTable table = new HTable(conf, Bytes.toBytes(tableName));
			table.setAutoFlush(false);
			byte[] family = Bytes.toBytes("f");
			byte columns[][] = {Bytes.toBytes("PV"),
					Bytes.toBytes("UV"),
					Bytes.toBytes("UPV")};
			Random rand = new Random();
			for (int i=0;i<10000;i++) {
				Put put = new Put(Bytes.toBytes(i)); //row key
				for (int j=0;j<columns.length;j++) {
					ByteBuffer value = ByteBuffer.allocate(3*4);
					//first put a 'count'
					value.put(Bytes.toBytes(2)); 
					//next put two integer number
					value.put(Bytes.toBytes(rand.nextInt())); 
					value.put(Bytes.toBytes(rand.nextInt()));
					put.add(family, columns[j], value.array());
				}
				table.put(put);
			}
			table.flushCommits();
			table.close();
		} catch (Exception ex) {
			LOG.error("exception", ex);
		} 
	}
	
	public static void main(String[] args) {
		DataFactory df = new DataFactory();
		//1. create table
		df.createTable("TEST", "f");
		//2. put data
		df.artificalData("TEST");
	}
}
