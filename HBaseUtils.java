package com.suning.dfp.td.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseUtils {
    private static final Logger LOG= LoggerFactory.getLogger(HBaseUtils.class);
    static {
        PropertyConfigurator.configureAndWatch("log4j.properties",10000);
    }
    private static Object object=new Object();
    private static org.apache.hadoop.conf.Configuration conf;
    private static HConnection connection;

    public static HConnection getConnection(){
        if(null==connection){
            synchronized (object){
                if(null==connection){
                    conf= HBaseConfiguration.create();
                    System.getProperties().setProperty("HADOOP_USER_NAME",
                            Configuration.getString(Configuration.HADOOP_USER_NAME));
                    System.getProperties().setProperty("HADOOP_GROUP_NAME",
                            Configuration.getString(Configuration.HADOOP_GROUP_NAME));
                    conf.set("hbase.zookeeper.quorum", Configuration.getString(Configuration.HBASE_ZK_QUORUM));
                    conf.set("hbase.zookeeper.property.clientPort",
                            Configuration.getString(Configuration.HBASE_ZK_PROP_PORT));
                    conf.set("zookeeper.znode.parent",Configuration.getString(Configuration.HBASE_ZK_ZNODE_PARENT));
                    conf.set("hbase.client.retries.number","2");
                    conf.set("hbase.client.pause","100");
                    conf.set("zookeeper.recovery.retry.intervalmill","200");
                    conf.set("ipc.socket.timeout","2000");
                    conf.set("hbase.rpc.timeout","2000");
                    conf.set("hbase.client.scanner.timeout.period","2000");
                    conf.set("hbase.client.operation.timeout","5000");
                    try {
                        connection= HConnectionManager.createConnection(conf);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }

    public static HConnection getConnection(String hadoopUserName, String hadoopGroupName, String hbaseZkQuorum,
                                            String hbaseZkPropPort, String hbaseZkZnodeParent) throws IOException {
        HConnection connection = null;
        synchronized (object) {
            conf = HBaseConfiguration.create();
            System.getProperties().setProperty("HADOOP_USER_NAME", hadoopUserName);
            System.getProperties().setProperty("HADOOP_GROUP_NAME", hadoopGroupName);
            conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", hbaseZkPropPort);
            conf.set("zookeeper.znode.parent", hbaseZkZnodeParent);
            // 重试次数，默认为14，可配置为3
            conf.set("hbase.client.retries.number", "2");
            // 重试的休眠时间，默认为1s，可减少，比如100ms
            conf.set("hbase.client.pause", "100");
            // zk重试的休眠时间，默认为1s，可减少，比如：200ms
            conf.set("zookeeper.recovery.retry.intervalmill", "200");
            conf.set("ipc.socket.timeout", "2000");
            conf.set("hbase.rpc.timeout", "2000");
            conf.set("hbase.client.scanner.timeout.period", "2000");
            conf.set("hbase.client.operation.timeout", "5000");
            connection = HConnectionManager.createConnection(conf);
            LOG.info("2.connect hbase success!" + hbaseZkQuorum);
        }
        return connection;
    }
    public static Result selectByKey(String tabName,String key) throws IOException {
        Result result=null;
        HTableInterface table=getConnection().getTable(tabName);
        Get get=new Get(Bytes.toBytes(key));
        result=table.get(get);
        table.close();
        return result;
    }
    public static Result selectByKey(HConnection hConnection,String tabName,String key) throws IOException {
        Result result=null;
        if(null != hConnection){
            HTableInterface table=hConnection.getTable(tabName);
            Get get=new Get(Bytes.toBytes(key));
            result = table.get(get);
            table.close();
        }
        return result;
    }

    /**
     * 查询hbase表
     * @param tabName
     * @return
     * @throws IOException
     */
    public static ResultScanner scan(String tabName) throws IOException {
        HTableInterface table=getConnection().getTable(tabName);
        Scan scan=new Scan();
        ResultScanner rs=table.getScanner(scan);
        return rs;
    }
    public static ResultScanner scan(HConnection hConnection,String tabName) throws IOException {
        ResultScanner rs=null;
        if(null != hConnection){
            HTableInterface table=hConnection.getTable(tabName);
            Scan scan=new Scan();
            rs=table.getScanner(scan);
        }
        return rs;
    }

    /**
     * 往hbase数据库中插入单条数据
     * @param tabName
     * @param rowKey
     * @param colFamily
     * @param colName
     * @param map
     * @throws IOException
     */
    public static void insert(String tabName, String rowKey, String colFamily, String colName, Map<String,String> map) throws IOException {
        JSONObject obj=new JSONObject();
        HTable table= (HTable) getConnection().getTable(tabName);
        Put put=new Put(Bytes.toBytes(rowKey));
        for(Map.Entry<String,String> entry:map.entrySet()){
            obj.put(entry.getKey(),entry.getValue());
        }
        put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(obj.toString()));
        table.put(put);
        table.flushCommits();
        table.close();
    }
    public static void insert(HConnection hConnection,String tabName,String rowKey,String colFamily,String colName,Map<String,String> map) throws IOException {
        JSONObject obj=new JSONObject();
        HTable table= (HTable) hConnection.getTable(tabName);
        Put put=new Put(Bytes.toBytes(rowKey));
        for(Map.Entry<String,String> entry:map.entrySet()){
            obj.put(entry.getKey(),entry.getValue());
        }
        put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(obj.toString()));
        table.put(put);
        table.flushCommits();
        table.close();
    }
    public static void insert(String tabName,String rowkey,String colFamily,String colName,JSONObject obj) throws IOException {
        HTable table= (HTable) getConnection().getTable(tabName);
        Put put=new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(obj.toString()));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    /**
     * 将单条json数据插入hbase表中
     * @param hConnection
     * @param tabName
     * @param rowkey
     * @param colFamily
     * @param colName
     * @param obj
     * @return
     * @throws IOException
     */
    public static boolean insert(HConnection hConnection,String tabName,String rowkey,String colFamily,String colName,JSONObject obj) throws IOException {
        boolean flag=false;
        if(null != hConnection){
            HTable table= (HTable) hConnection.getTable(tabName);
            Put put=new Put(Bytes.toBytes(rowkey));
            put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(obj.toString()));
            table.put(put);
            table.flushCommits();
            table.close();
            flag=true;
        }
        return flag;
    }
    public static void insert(String tabName,String rowKey,String colFamily,String colName,String str) throws IOException {
        HTable table= (HTable) getConnection().getTable(tabName);
        Put put=new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(str));
        table.put(put);
        table.flushCommits();
        table.close();
    }
    public static void insert(HConnection hConnection,String tabName,String rowKey,String colFamily,String colName,String str) throws IOException {
        HTable table= (HTable) hConnection.getTable(tabName);
        Put put=new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(str));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    /**
     * 批量往hbase插入数据
     * @param tabName
     * @param colFamily
     * @param colName
     * @param map
     */
    public static void batchInsert(String tabName,String colFamily,String colName,Map<String,Map<String,String>> map) throws IOException {
        HTable table= (HTable) getConnection().getTable(tabName);
        table.setAutoFlush(false,true);
        table.setWriteBufferSize(24L*1024L*1024L);
        List<Put> list=new ArrayList<Put>();
        int index=0;
        for(Map.Entry<String,Map<String,String>> entry:map.entrySet()){
            Put put=new Put(Bytes.toBytes(entry.getKey()));
            Map<String,String> mapInfo=entry.getValue();
            JSONObject obj=new JSONObject();
            for(Map.Entry<String,String> entryItem:mapInfo.entrySet()){
                obj.put(entryItem.getKey(),entryItem.getValue());
            }
            put.add(Bytes.toBytes(colFamily),Bytes.toBytes(colName),Bytes.toBytes(obj.toString()));
            list.add(put);
            if(list.size()%1000==0){
                table.put(list);
                list.clear();
                table.flushCommits();
            }
        }
        if (list.size()>0){
            table.put(list);
            list.clear();
            table.flushCommits();
            table.close();
        }
    }

    /**
     * 删除对应表的主键信息
     * @param tabName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteByKey(String tabName,String rowKey) throws IOException {
        HTable table= (HTable) getConnection().getTable(tabName);
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }
    public static void deleteByKey(HConnection hConnection,String tabName,String rowKey) throws IOException {
        if(null != hConnection) {
            HTable table = (HTable) hConnection.getTable(tabName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }
    }
}
