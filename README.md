#Introduction

Are you crazy about the hbase shell output like below? You have no idea what the hell the hex string is, right?   

![hbase shell](https://github.com/xianli/hbasewebnavi/raw/master/src/main/webapp/images/hbaseshell.PNG "hbase shell")  
 
Please try the HBase Table Navigator, it is a lightweight and extensible web app. you can get readable output from hbase table. it provides three functions:  
1. get  
2. scan  
3. count  
Below **picture** demostrates how the 'scan' works. In the result section of the picture, 
it lists 20 random rows in hbase table 'TEST'. only 4 rows are visible in the picture, others are truncated. 
more user friendly than hbase shell, isn't it? :)
![demo scan](https://github.com/xianli/hbasewebnavi/raw/master/src/main/webapp/images/demo_scan.PNG "demo scan")

#How it works?

I don't want to put many words on the web app as it is simple. The core of the app is an **Interpreter** interface,
it has two methods:  

`String interpretRowkey(String tableName, byte[] rowkey);`   
`String interpretColumnValue(String tableName, byte[] rowkey, byte[] cf, byte[] column, long timestamp, byte[] value);`  

the web app framework will call these two methods to interpret the row key, column value in hbase table and convert them
to human readable string format. 

Since different application stores data in different format, so you may want to add your own **Interpreter**. Please edit below 
line in **conf.properties** to add your own Interpreter.   
`interpreter.class=com.xl.hbase.InterpreterImpl`

#Installation
Install this app is very easy,   
1. create a web app in eclipse.  
2. import the source code in this repository.   
3. add your own Interpreter.  
4. export a war and deploy it to any web server.  
5. give it a try via [http://yourhost/hbasewebnavi/verify.jsp](http://yourhost/hbasewebnavi/verify.jsp).  
