package com.huawei.bigdata.kafka.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaProperties
{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProperties.class);
    
    // Topic名称，安全模式下，需要以管理员用户添加当前用户的访问权限
    private static String TOPIC;

	private static String FILENAME;
	private static String PATH_TMP;
	private static String PATH;
	public static final int FILE_LEN = 100;
    
    private static Properties serverProps = new Properties();
    private static Properties producerProps = new Properties();
    
    private static Properties consumerProps = new Properties();
    
    private static Properties clientProps = new Properties();
    
    private static KafkaProperties instance = null;
    
    private KafkaProperties()
    {
        String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        
        try
        {
            File proFile = new File(filePath + "producer.properties");
        
            if (proFile.exists())
            {
                producerProps.load(new FileInputStream(filePath + "producer.properties"));
            }
        
            File conFile = new File(filePath + "producer.properties");
        
            if (conFile.exists())
            {
                consumerProps.load(new FileInputStream(filePath + "consumer.properties"));
            }
        
            File serFile = new File(filePath + "server.properties");
            
            if (serFile.exists())
            {
            	serverProps.load(new FileInputStream(filePath + "server.properties"));
            }

            File cliFile = new File(filePath + "client.properties");
            
            if (cliFile.exists())
            {
            	clientProps.load(new FileInputStream(filePath + "client.properties"));
            }
            
            TOPIC = getValues("topic", "rtmp_publish");
            FILENAME = getValues("filename", "rtmp_publish");
            PATH = getValues("path", "");
            PATH_TMP = getValues("path_tmp", "");
        }
        catch (IOException e)
        {
            LOG.info("The Exception occured.", e);
        }
    }
    
    public synchronized static KafkaProperties getInstance()
    {
        if (null == instance)
        {
            instance = new KafkaProperties();
        }
        
        return instance;
    }
    
    public static String getTopic(){
    	return TOPIC;
    }
    
    public static String getFilename(){
    	return FILENAME;
    }
    
    public static String getPath(){
    	return PATH;
    }
    
    public static String getPathTmp(){
    	return PATH_TMP;
    }
    
    /**
    * 获取参数值
    * @param key properites的key值
    * @param defValue 默认值
    * @return
    */
    public String getValues(String key, String defValue)
    {
        String rtValue = null;
        
        if (null == key)
        {
            LOG.error("key is null");
        }
        else
        {
            rtValue = getPropertiesValue(key);
        }
        
        if (null == rtValue)
        {
            LOG.warn("KafkaProperties.getValues return null, key is " + key);
            rtValue = defValue;
        }
        
        LOG.info("KafkaProperties.getValues: key is " + key + "; Value is " + rtValue);
        
        return rtValue;
    }
    
    /**
    * 根据key值获取server.properties的值
    * @param key
    * @return
    */
    private String getPropertiesValue(String key)
    {
        String rtValue = serverProps.getProperty(key);
        
        // server.properties中没有，则再向producer.properties中获取
        if (null == rtValue)
        {
            rtValue = producerProps.getProperty(key);
        }
        
        // producer中没有，则再向consumer.properties中获取
        if (null == rtValue)
        {
            rtValue = consumerProps.getProperty(key);
        }
        
        // consumer没有，则再向client.properties中获取
        if (null == rtValue)
        {
        	rtValue = clientProps.getProperty(key);
        }
        
        return rtValue;
    }
}
