package com.sree.kafka.connectors.jmx;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

/**
 *
 * JMX Client class which can initialize connection to remote JMX Server
 *
 * @author sree
 *
 *
 */

public class JmxClient {
	private static Logger logger = LoggerFactory.getLogger(JmxClient.class);
	JMXConnector jmxConnector;
	public MBeanServerConnection mBeanServer;
	private String jmxUrl;
	private Map jmxProps;


	public void configure(String jmxUrl, Map environment){
		this.jmxUrl=jmxUrl;
		this.jmxProps=environment;
	}

	/**
	 * Instantiate JMX connection
	 *
	 * @param jmxUrl
	 * @param environment
	 */
	public void initializeJmxClient() {

		try {
			System.setProperty("java.rmi.server.hostname", "localhost");
			jmxConnector = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), jmxProps);
			mBeanServer = jmxConnector.getMBeanServerConnection();
			logger.info("JMX Connection and MBean Server successfully initialized to " + jmxUrl);
		} catch (MalformedURLException e) {
			logger.error("Malformed JMX URL Exception : ", e);
		} catch (IOException e) {
			logger.error("IO Exception : ", e);
		}

	}

	public boolean isConnected(){
		//TODO improve this
		return jmxConnector!=null && mBeanServer!=null;
	}

	/**
	 * Close JMX connection
	 */
	public void close() {
		try {
			if(jmxConnector!=null)
				jmxConnector.close();

		} catch (IOException e) {
			logger.error("JMX Connector closing exception  : ", e);
		}finally {
			jmxConnector=null;
			mBeanServer=null;
		}
	}

	/**
	 * Get the complete list of beans registered in JMX
	 *
	 * @param mbeanConnection
	 * @return
	 */
	public Set getCompleteMbeans() {
		Set beans = new HashSet();
		try {
			if(!isConnected()){
				logger.error("Error connecting to service at {} with props  ",jmxUrl);
				logger.info("Re connecting to service at {}",jmxUrl);
				initializeJmxClient();
			}
			if(isConnected())
				beans = mBeanServer.queryNames(null, null);
		} catch (IOException e) {
			close();
			logger.error("Not able to get complete beans {} ", e);
		}
		return beans;
	}

	/**
	 * Get the list of beans for white listed domains registered in JMX
	 *
	 * @param mbeanConnection
	 * @param mWhiteListedDomains
	 * @return
	 */
	public Set getCompleteMbeans(List<String> mWhiteListedDomains) {
		Set<ObjectName> beans = getCompleteMbeans();
		Set<ObjectName> filteredBeans=new HashSet<>();
		for(ObjectName objectName:beans)
			if(mWhiteListedDomains.contains(objectName.getDomain()))filteredBeans.add(objectName);
		return filteredBeans;
	}


	public List<String> getMetricsFromMbean(Set beans, String serviceName) {
		List<String> beanList = new ArrayList<String>();
		JSONArray jsonArray=new JSONArray();
		for (Object obj : beans) {
			try {
				JSONObject jsonObject = new JSONObject();
				ObjectName beanName = null;
				if (obj instanceof ObjectName)
					beanName = (ObjectName) obj;
				else if (obj instanceof ObjectInstance)
					beanName = ((ObjectInstance) obj).getObjectName();

				jsonObject.put("domain", beanName.getDomain());
				Hashtable<String, String> properties = beanName.getKeyPropertyList();
				for (Map.Entry<String, String> prop : properties.entrySet()) {
					jsonObject.put(prop.getKey(),prop.getValue());
				}

				// Get all attributes and its values
				try {
					MBeanInfo beanInfo = mBeanServer.getMBeanInfo(beanName);
					MBeanAttributeInfo[] attrInfo = beanInfo.getAttributes();
					HashMap<String,Object> attribs=new HashMap<>();
					for (MBeanAttributeInfo attr : attrInfo) {
						try {
							attribs.put(attr.getName(),
									mBeanServer.getAttribute(beanName, attr.getName()).toString());
						} catch (Exception e) {
							logger.error("Attr parsing exception {} ", e);
						}
					}
					jsonObject.put("attributes",attribs);
				} catch (Exception e) {
					logger.error("Get Bean attribute exception {} ", e);
				}
				jsonArray.put(jsonObject);
			} catch (Exception e) {
				logger.error("Get Metrics From bean exception {} ", e);
			}
		}
		JSONObject jsonObj=new JSONObject();
		//TODO make it configurable
		jsonObj.put("sourceType","Connect");
		//jsonObj.put("version",1);
		jsonObj.put("serviceName",serviceName);
		jsonObj.put("beans",jsonArray);
		beanList.add(jsonObj.toString());
		return beanList;
	}




}
