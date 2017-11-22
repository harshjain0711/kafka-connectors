package com.sree.kafka.connectors.jmx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sree.kafka.connectors.jmx.config.JmxConfigs;
import com.sree.kafka.connectors.jmx.config.JmxConstants;
import com.sree.kafka.utils.TextUtils;
import com.sree.kafka.utils.Version;
import com.sree.kafka.utils.connect.ConnectMessageTemplate;

/**
 *
 * Kafka JMX Task which will - Establish JMX connection - Get all available
 * MBeans - Filter MBeans based on jmx.servicename - Create corresponding JSON -
 * Push the output JSON to kafka.topic
 *
 * @author sree
 *
 *
 */

public class JmxTask extends SourceTask {
	private static final Logger logger = LoggerFactory.getLogger(JmxTask.class);
	BlockingQueue<ConnectMessageTemplate> jmxQueue = new LinkedBlockingQueue<ConnectMessageTemplate>();
	JmxConfigs jmxConfig;
	String kafkaTopic;
	String userName;
	String password;
	long jmxWaitTimeout;
	long jmxFetchTimeout;
	long rmiConnectTimeout;
	long rmiHandshakeTimeout;
	long rmiResponseTimeout;
	String jmxService;
	JmxClient jmxClient;
	private long lastPollMillis;
	private long pollIntervalMillis;
	private Time time;
	private AtomicBoolean stop;
	private List<String> whiteListedDomainList;

	public JmxTask(){
		this.time = new SystemTime();
		lastPollMillis=time.milliseconds();
	}

	/**
	 * Get Version of the connector
	 */
	public String version() {
		return Version.getVersion();
	}

	/**
	 * This method repeatedly call at a certain interval Get all the JSON JMX
	 * metrics Insert it to Queue Poll from Queue and push it to kafka.topic
	 */
	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = new ArrayList<SourceRecord>();

		waitPollInterval();

		//shutdown called while in sleep state
		if(stop.get())return null;

		try {
			Set beans = null;
			if(whiteListedDomainList.size()>0)
				beans=jmxClient.getCompleteMbeans(whiteListedDomainList);
			else
				beans=jmxClient.getCompleteMbeans();
			List<String> metricsOutput = new ArrayList<String>();
			try {
				metricsOutput = jmxClient.getMetricsFromMbean(beans, jmxService);
				for (String bean : metricsOutput) {
					try {
						ConnectMessageTemplate jmxMessage = new ConnectMessageTemplate();
						jmxMessage.setMessage(bean);
						jmxMessage.setKafkaTopic(kafkaTopic);
						jmxQueue.add(jmxMessage);
					} catch (Exception e) {
						logger.error("Adding bean output to JmxMessageProcessor failed {} ", e);
					}
				}
			} catch (Exception e) {
				logger.error("Create Metrics output exception {} ", e);
			}
		} catch (Exception e) {
			logger.error("JMX bean source record creation exception {} ", e);
		}
		while (!jmxQueue.isEmpty()) {
			ConnectMessageTemplate jmxMsg = jmxQueue.poll();
			records.add(jmxMsg.realTimeMesssageToSourceRecord());
		}
		lastPollMillis=time.milliseconds();
		return records;
	}


	private void waitPollInterval() {
		final long nextUpdate = lastPollMillis + pollIntervalMillis;
		final long untilNext = nextUpdate - time.milliseconds();
		if (untilNext > 0) {
			logger.info("Waiting {} ms to poll next metrics snapshot", untilNext);
			time.sleep(untilNext);
		}
	}
	/**
	 * Configuration and global variables initialization
	 */
	@Override
	public void start(Map<String, String> props) {
		jmxClient = new JmxClient();
		try {
			jmxConfig = new JmxConfigs(props);
		} catch (ConfigException e) {
			logger.error("Couldn't start " + JmxConnector.class.getName() + " due to configuration error.", e);
		}
		kafkaTopic = jmxConfig.getString(JmxConstants.KAFKA_TOPIC);
		jmxService = jmxConfig.getString(JmxConstants.SERVICE);
		pollIntervalMillis=jmxConfig.getLong(JmxConstants.POLL_INTERVAL_MS_CONFIG);
		whiteListedDomainList=Arrays.asList(jmxConfig.getString(JmxConstants.DOMAIN_WHITELIST).split(","));
		initializeJmxConnector();
		stop = new AtomicBoolean(false);
	}

	/**
	 * Initialize JMX connector for any other services jmx.servicename is
	 * flink/redis/cassandra etc get the connection to each jmx.url
	 */
	private void initializeJmxConnector() {
		String jmxUrl = jmxConfig.getString(JmxConstants.JMX_URL);
		initializeJmxClient(jmxUrl);
	}




	private void initializeJmxClient(String host) {
		try {
			jmxClient.configure(generateJmxURL(host),
					generateJmxEnvironment());
			jmxClient.initializeJmxClient();
		} catch (Exception e) {
			logger.error("JMX Client connection exception {} ", e);
		}
	}

	/**
	 * Generate JMX URL for each kafka brokers discovered using zookeeper
	 *
	 * @return
	 */
	private String generateJmxURL(String kafkaBroker) {
		String jmxURL = "";
		try {
			jmxURL = "service:jmx:rmi:///jndi/rmi://" + kafkaBroker + "/jmxrmi";
		} catch (Exception e) {
			logger.error("JMX URL generation failed : ", e);
		}
		return jmxURL;
	}

	/**
	 * Create JMX Environment and Timeout settings
	 *
	 * @return
	 */
	private Map generateJmxEnvironment() {
		Map jmxEnv = new HashMap();
		try {
			jmxWaitTimeout = jmxConfig.getLong(JmxConstants.JMX_WAIT_TIMEOUT);
			jmxFetchTimeout = jmxConfig.getLong(JmxConstants.JMX_FETCH_TIMEOUT);
			rmiConnectTimeout = jmxConfig.getLong(JmxConstants.RMI_CONNECT_TIMEOUT);
			rmiHandshakeTimeout = jmxConfig.getLong(JmxConstants.RMI_HANDSHAKE_TIMEOUT);
			rmiResponseTimeout = jmxConfig.getLong(JmxConstants.RMI_RESPONSE_TIMEOUT);

			jmxEnv.put("jmx.remote.x.request.waiting.timeout", Long.toString(jmxWaitTimeout));
			jmxEnv.put("jmx.remote.x.notification.fetch.timeout", Long.toString(jmxFetchTimeout));
			jmxEnv.put("sun.rmi.transport.connectionTimeout", Long.toString(rmiConnectTimeout));
			jmxEnv.put("sun.rmi.transport.tcp.handshakeTimeout", Long.toString(rmiHandshakeTimeout));
			jmxEnv.put("sun.rmi.transport.tcp.responseTimeout", Long.toString(rmiResponseTimeout));

			userName = jmxConfig.getString(JmxConstants.JMX_USERNAME);
			password = jmxConfig.getString(JmxConstants.JMX_PASSWORD);

			if (!TextUtils.isNullOrEmpty(userName) && !TextUtils.isNullOrEmpty(password)) {
				String[] creds = { userName, password };
				jmxEnv.put(JMXConnector.CREDENTIALS, creds);
			}
		} catch (Exception e) {
			logger.error("JMX Environment Exception ", e);
		}

		return jmxEnv;
	}

	/**
	 * Stop the JMX Task
	 */
	@Override
	public void stop() {
		if (stop != null) {
			stop.set(true);
		}
		logger.info("Stopping JMX Kafka Source..");
		jmxClient.close();
	}
}
