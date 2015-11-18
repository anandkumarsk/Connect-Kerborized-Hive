package com.sample.packagename;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;


import java.io.IOException;


public class AccessDB {

	public void insertDataHbase(String rowkey, String topic, String fileid, String to, String emailsentAt) throws IOException
	{
		Configuration conf = HBaseConfiguration.create();
		HTable table=null;
		try {
			table = new HTable(conf,"tablename or Path");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Put p1 = new Put(rowkey.getBytes());
		
		
		byte[] Kafka = "Kafka".getBytes();
		byte[] File = "File".getBytes();
		byte[] Email = "Email".getBytes();
		byte[] DtTime = "DtTime".getBytes();
		
		p1.add(Kafka,"topic".getBytes(),topic.getBytes());
		
		p1.add(File,"fileid".getBytes(),fileid.getBytes());
		
		p1.add(Email,"to".getBytes(),to.getBytes());
		
		p1.add(DtTime,"emailsentAt".getBytes(),emailsentAt.getBytes());
		
		try {
			System.out.println("put to table is called");
			table.put(p1);
		//	table.put(p2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Scan s = new Scan();
        s.addColumn(Bytes.toBytes("File"), Bytes.toBytes("fileid"));
        

        
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("rowkey value - hardcode"));
        s.setFilter(filter);
        
        ResultScanner scanner = table.getScanner(s);
        try {
           // Scanners return Result instances.
           // Now, for the actual iteration. One way is to use a while loop like so:
           for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
             // print out the row we found and the columns we were looking for
             System.out.println("Found row: " + rr);
             
             System.out.println("Found the file ID already so not sending email " + rr);
           }

           // The other approach is to use a foreach loop. Scanners are iterable!
           // for (Result rr : scanner) {
           //   System.out.println("Found row: " + rr);
           // }
         } finally {
           // Make sure you close your scanners when you are done!
           // Thats why we have it inside a try/finally clause
           scanner.close();
         }
		
		
		
		
		
		
		try {
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
	}
	
	
	public boolean readDataHbase(String rowkey, String topic, String fileid, String to, String emailsentAt) throws IOException
	{
		Configuration conf = HBaseConfiguration.create();
		HTable table=null;
		boolean fileid_found=false;
		try {
			table = new HTable(conf,"sample hbase table name or path");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scan s = new Scan();
        s.addColumn(Bytes.toBytes("File"), Bytes.toBytes("fileid"));
        
        
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(fileid));
        
        
        s.setFilter(filter);
        
        ResultScanner scanner = table.getScanner(s);
        try {
           // Scanners return Result instances.
           // Now, for the actual iteration. One way is to use a while loop like so:
           for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
             // print out the row we found and the columns we were looking for
             System.out.println("Found row: " + rr);
             
             System.out.println("Found the file ID already so not sending email " + rr);
             
             fileid_found=true;
             
           }

           // The other approach is to use a foreach loop. Scanners are iterable!
           // for (Result rr : scanner) {
           //   System.out.println("Found row: " + rr);
           // }
         } finally {
           // Make sure you close your scanners when you are done!
           // Thats why we have it inside a try/finally clause
           scanner.close();
         }
		
		
		
		
		
		
		try {
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return fileid_found;
		
		
		 
		
	}
	
	
}//class access-DB
