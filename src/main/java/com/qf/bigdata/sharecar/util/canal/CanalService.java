package com.qf.bigdata.sharecar.util.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.google.protobuf.ByteString;
import com.qf.bigdata.sharecar.constant.CommonConstant;
import com.qf.bigdata.sharecar.enumes.DBColumnTypeEnum;
import com.qf.bigdata.sharecar.util.CommonUtil;
import com.qf.bigdata.sharecar.util.jdbc.JDBCUtil;
import com.qf.bigdata.sharecar.util.kafka.producer.KafkaProducerUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * Created by finup on 2018/11/26.
 */
public class CanalService {

    private final static Logger log = LoggerFactory.getLogger(CanalService.class);

    //canal-mysql-batchid
    public static final long DEF_BATCHID = -1l;

    //时间格式（时区）
    public static final String DEF_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    //canal-mysql配置
    public static final String CANAL_MYSQL_DB_CONF = "canal/jdbc.properties";

    //pk id
    public static final String CANAL_BINLOG_PK_ID = "id";

    //操作类型
    public static final String CANAL_BINLOG_OPT = "log_opt";

    //binlog schema
    public static final String CANAL_SCHEMA = "db_table";

    private static List<CanalConnection> connections;

    private static Map<String,String> insTables;

    public static Map<String,String> getInsTables(String confPath) throws Exception{
        if(null == insTables){
            insTables = JDBCUtil.jdbc4Ins(confPath);
        }
        log.info(String.format("insTables=%s", insTables));
        return insTables;
    }


    /**
     * 获取增量数据（binlog）
     * @throws Exception
     */
    public static void getIncreamentData() throws Exception{
        try{
            connections = CanalUtil.createConnections();
            for(CanalConnection connection: connections){
                handleConnection(connection);
            }
        }catch (Exception e){
            log.error("CanalService.getIncreamentData.err:",e);
            throw new Exception("CanalService.getIncreamentData.err");
        }
    }

    /**
     * 数据消费
     * @throws Exception
     */
    public static  boolean handleConsume(Map<String,String> msgDatas) throws Exception{
        boolean consumeResult = false;
        try{
            if(null != msgDatas){
                String topic = CommonConstant.TOPIC_BINLOG1;
                KafkaProducerUtil.sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, msgDatas);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return consumeResult;
    }


    /**
     * 增量数据处理
     * @param connection
     * @throws Exception
     */
    public static void handleConnection(CanalConnection connection) throws Exception{
        CanalConnector connector = null;
        long batchId = DEF_BATCHID;
        //batchId=152;
        try{
            if(null == connection){
                throw new Exception("CanalService.handleIncreamentData.connection is null");
            }

            CanalConf conf = connection.getConf();
            connector = connection.getConnector();
            String subscribe = conf.getCanalFilter();
            Integer batchSize = conf.getCanalBatchSize();

            connector.connect();
            connector.subscribe(subscribe);
            connector.rollback();
            log.info(String.format("CanalService.connection=%s", connection));
            while (true) {
                Message message = connector.getWithoutAck(batchSize); //batchSize 获取指定数量的数据
                String messageJson = message.toString();
                System.out.println("messageJson="+messageJson);
                log.info(messageJson);

                batchId = message.getId();
                int size = message.getEntries().size();

                if(batchId == DEF_BATCHID || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.error("CanalService.handleIncreamentData.err:",e);
                    }
                }else{
                    Map<String,String> msgDatas = handleData(message);
                    boolean consumeResult = handleConsume(msgDatas);
                    if(consumeResult){
                        //提交确认
                        connector.ack(batchId);
                        System.out.printf("message.ack[batchId=%s] \n", batchId);
                    }
                }
            }

        }catch (Exception e){
            log.error("CanalService.handleIncreamentData.err:",e);
            if(null != connector){
                // 处理失败, 回滚数据
                connector.rollback(batchId);
            }
            throw new Exception("CanalService.handleIncreamentData.err");
        }finally {
//            if(null != connector){
//                connector.disconnect();
//            }
        }
    }


    /**
     * 获取表的唯一列
     * @param schemaName
     * @param tableName
     * @return
     */
    public static String getPIDColumn(String schemaName, String tableName) {
        if(StringUtils.isEmpty(schemaName)){
            return null;
        }
        if(StringUtils.isEmpty(tableName)){
            return null;
        }

        String dbTableName = schemaName + "." +tableName;
        return dbTableName;
    }


    /**
     * 处理数据
     * @throws Exception
     */
    public static List<CanalJson> handleIncreamentData(Message message) throws Exception{

        List<CanalJson> result = new ArrayList<CanalJson>();
        if(null == message){
            throw new Exception("CanalService.handleIncreamentData.message is null");
        }

        List<Entry> entrys = message.getEntries();
        for(CanalEntry.Entry entry : entrys) {
            String entryJson = entry.toString();
            //System.out.println("entryJson="+entryJson);
            log.info(entryJson);

            //日志操作类型
            EntryType entryType = entry.getEntryType();

            //事务日志
            if(entryType == EntryType.TRANSACTIONBEGIN || entryType == EntryType.TRANSACTIONEND) {
                continue;
            }

            //日志文件信息
            CanalEntry.Header header = entry.getHeader();
            String logFileName = header.getLogfileName();
            long logFileOffset = header.getLogfileOffset();
            String schemaName = header.getSchemaName();

            //表
            String tableName = header.getTableName();
            String pidColumn = getPIDColumn(schemaName, tableName);
            log.info(String.format("binlog logFileName=%s, logFileOffset=%s, schemaName=%s, tableName=%s", logFileName, logFileOffset, schemaName, tableName));


            //数据 ROWDATA
            ByteString rowByteString = entry.getStoreValue();
            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(rowByteString);

                //事件类型
                EventType eventType = rowChage.getEventType();
                for (RowData rowData : rowChage.getRowDatasList()) {
                    //插入、修改
                    if(eventType == EventType.INSERT || eventType == EventType.UPDATE){
                        List<Column> columns = rowData.getAfterColumnsList();
                        Map<String, Object> columnMap = convertColumn2Map(columns);
                        if(null != columnMap){
                            Object idObj = columnMap.getOrDefault(CANAL_BINLOG_PK_ID,null);
                            if(null != idObj){
                                String id = idObj.toString();
                                String columnJson = JSONObject.toJSONString(columnMap);
                                //System.out.println("column.map=" + columnJson);
                                //log.info(String.format("column.map=%s",JSONObject.toJSONString(columnMap)));

                                CanalJson canalJson = new CanalJson();
                                canalJson.setTableName(pidColumn);
                                canalJson.setId(id);
                                canalJson.setDatas(columnJson);
                                result.add(canalJson);
                            }
                        }
                    }else if(eventType == EventType.DELETE){
                        log.info(String.format("binlog.change.eventType=%s",eventType));
                    }else if(eventType == EventType.CREATE || eventType == EventType.ALTER){//ddl
                        log.info(String.format("binlog.change.eventType=%s",eventType));
                    }else {
                        log.info(String.format("binlog.change.eventType=%s",eventType));
                    }
                }
            } catch (Exception e) {
                log.error("CanalService.handleIncreamentData.error=>", e);
                throw new RuntimeException("CanalService.handleIncreamentData.parse from eromanga-event has an error, data:" + entry.toString(), e);
            }
        }

        return result;
    }


    /**
     * 处理数据
     * @throws Exception
     */
    public static Map<String,String> handleData(Message message) throws Exception{

        Map<String,String> result = new HashMap<String,String>();
        if(null == message){
            throw new Exception("CanalService.handleIncreamentData.message is null");
        }

        List<Entry> entrys = message.getEntries();
        for(CanalEntry.Entry entry : entrys) {
            String entryJson = entry.toString();
            //System.out.println("entryJson="+entryJson);
            log.info(entryJson);

            //日志操作类型
            EntryType entryType = entry.getEntryType();

            //事务日志
            if(entryType == EntryType.TRANSACTIONBEGIN || entryType == EntryType.TRANSACTIONEND) {
                continue;
            }


            //日志文件信息
            CanalEntry.Header header = entry.getHeader();
            String logFileName = header.getLogfileName();
            long logFileOffset = header.getLogfileOffset();
            String schemaName = header.getSchemaName();

            //表
            String tableName = header.getTableName();
            String schemaColumn = getPIDColumn(schemaName, tableName);
            log.info(String.format("binlog logFileName=%s, logFileOffset=%s, schemaName=%s, tableName=%s", logFileName, logFileOffset, schemaName, tableName));


            //数据 ROWDATA
            ByteString rowByteString = entry.getStoreValue();
            RowChange rowChage = RowChange.parseFrom(rowByteString);


            //事件类型
            EventType eventType = rowChage.getEventType();
            for (RowData rowData : rowChage.getRowDatasList()) {
                //插入、修改
                if(eventType == EventType.INSERT || eventType == EventType.UPDATE  || eventType == EventType.DELETE){
                    List<Column> columns = null;
                    if(eventType == EventType.INSERT || eventType == EventType.UPDATE ){
                        columns = rowData.getAfterColumnsList();
                    }else{
                        columns = rowData.getBeforeColumnsList();
                    }
                    Map<String, Object> columnMap = convertColumn2Map(columns);
                    if(null != columnMap){
                        columnMap.put(CANAL_BINLOG_OPT, eventType);
                        columnMap.put(CANAL_SCHEMA, schemaColumn);

                        Object idObj = columnMap.getOrDefault(CANAL_BINLOG_PK_ID,null);
                        if(null != idObj){
                            String id = idObj.toString();
                            String columnJson = JSONObject.toJSONString(columnMap);
                            String key = schemaColumn + CommonConstant.BOTTOM_LINE + id;

                            result.put(key,columnJson);
                        }
                    }
                }else if(eventType == EventType.CREATE || eventType == EventType.ALTER){//ddl
                    log.info(String.format("binlog.change.eventType=%s",eventType));

                    entry.hasEntryType();

                }else {
                    log.info(String.format("binlog.change.eventType=%s",eventType));
                }
            }
        }

        return result;
    }


    /**
     * 变动记录
     * @param columns
     * @return
     */
    private static Map<String, Object> convertColumn2Map(List<Column> columns) throws Exception{
        Map<String, Object> result = new HashMap<String, Object>();

        StringBuffer idValueBuffer = new StringBuffer();
        for (Column column : columns) {
            boolean isKey = column.getIsKey();
            boolean isNull = column.getIsNull();
            String mysqlType = column.getMysqlType();
            String name = column.getName();
            String value = column.getValue();

            //数据类型
            if (mysqlType.contains(DBColumnTypeEnum.TIMESTAMP.getCode()) || mysqlType.contains(DBColumnTypeEnum.DATETIME.getCode())) {
                if (StringUtils.isNotEmpty(value)) {
                    Date date = CommonUtil.parseText4Def(value);
                    //result.put(name, DateFormatUtils.format(date, DEF_PATTERN) + "+0800");
                    result.put(name, date.getTime());
                }
            }else{
                result.put(name, value);
            }

            //主键设置
            if(isKey){
                result.put(CANAL_BINLOG_PK_ID, value);
            }
        }

        return result;
    }





    public static void main(String args[]) throws Exception{

        CanalService service = new CanalService();

        service.getIncreamentData();

        //tt();

    }


}
