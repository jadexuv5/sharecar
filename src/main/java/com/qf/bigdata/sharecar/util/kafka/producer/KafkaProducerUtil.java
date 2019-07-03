package com.qf.bigdata.sharecar.util.kafka.producer;


import com.qf.bigdata.sharecar.constant.CommonConstant;
import com.qf.bigdata.sharecar.domain.Locus;
import com.qf.bigdata.sharecar.dvo.UovDO;
import com.qf.bigdata.sharecar.util.CommonUtil;
import com.qf.bigdata.sharecar.util.GisUtil;
import com.qf.bigdata.sharecar.util.kafka.KafkaUtil;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Created by hp on 2017/5/19.
 */
public class KafkaProducerUtil {

    private final static Logger log = LoggerFactory.getLogger(KafkaProducerUtil.class);

    /**
     * json数据
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg(String path, String topic, Map<String,String> datas) throws Exception{
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");

        KafkaProducer producer = KafkaUtil.createProducer(path);
        if(null != producer){
            List<String> lines = new ArrayList<String>();
            for(Map.Entry<String, String> entry : datas.entrySet()){
                String key = entry.getKey();
                String value = entry.getValue();

                log.info("Producer.key=" + key + ",value=" + value);
//                String line  = "time="+ CommonUtil.formatDate4Def(new Date())+",key="+key+"],value="+value;
////                lines.add(line);

                producer.send(new ProducerRecord<String, String>(topic, key, value));
                producer.flush();
            }
            producer.close();
        }

    }




    /**
     * kryo序列化
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg4Kryo(String path, String topic, Map<String,Serializable> datas){
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");

        try{
            Properties props = KafkaUtil.readKafkaProps(path);
            KafkaProducer<String, Serializable>  producer = new KafkaProducer<String, Serializable>(props);

            if(null != producer){

                for(Map.Entry<String, Serializable> entry : datas.entrySet()){
                    String key = entry.getKey();
                    Serializable value = entry.getValue();
                    System.out.println("Producer["+topic+"].key=" + key + ",value=" + value);

                    producer.send(new ProducerRecord<String, Serializable>(topic, key, value));
                    producer.flush();
                }

                producer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }



    //==========================================================================

    public static void testJson4While(String topic, int locusCount,String beginDay, String endDay, String dayBegin,String dayEnd,long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

        //时间(天)范围轨迹数据
        int diffDay = CommonUtil.calculateTimeDiffDay(dayFormatter, beginDay, endDay, Calendar.DATE).intValue();
        for(int i=0;i<=diffDay;i++){
            String curDay = CommonUtil.computeFormatTime(beginDay, i, Calendar.DATE, dayFormatter);
            String btStr = curDay+dayBegin;
            String etStr = curDay+dayEnd;
            List<UovDO> uovs = new ArrayList<UovDO>();

            //每天的轨迹数据
            int diff = CommonUtil.calculateTimeDiffDay(dateFormatter, btStr, etStr, Calendar.MINUTE).intValue();
            for(int z=1;z<diff;z++){
                String curTime = CommonUtil.computeFormatTime(btStr, z, Calendar.MINUTE, dateFormatter);
                for(int j=1;j<locusCount;j++){
                    UovDO uovDO = new UovDO();
                    String userCode = CommonUtil.getRandomNumStr(6);
                    String vehicleCode = CommonUtil.getRandomNumStr(4);
                    String orderCode = curTime+userCode+vehicleCode;
                    uovDO.setUserCode(userCode);
                    uovDO.setVehicleCode(vehicleCode);
                    uovDO.setOrderCode(orderCode);
                    uovs.add(uovDO);
                }

                Map<String,String> shareCarDatas = Locus.createDatas(uovs, btStr,etStr);
                sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, shareCarDatas);
                System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
                Thread.sleep(sleep);
            }
        }
    }



    public static void main(String[] args) throws  Exception {

        String topic = CommonConstant.TOPIC_SHARE_CAR_TEST;
        long sleep = 100 * 1;
        int locusCount = 10;
        String beginDay = "20190628";
        String endDay = "20190628";
        String dayBegin = "142000";
        String dayEnd = "143000";
        testJson4While(topic, locusCount, beginDay, endDay,dayBegin, dayEnd,sleep);
        System.out.println("Game Over!");

    }
}
