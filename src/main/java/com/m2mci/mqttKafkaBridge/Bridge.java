package com.m2mci.mqttKafkaBridge;

import static com.m2mci.mqttKafkaBridge.SslFactory.getSocketFactory;

import java.time.LocalTime;
import java.util.Properties;

import javax.net.ssl.SSLSocketFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.kohsuke.args4j.CmdLineException;

public class Bridge implements MqttCallback {
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	private	MqttClient mqtt;

	private Producer<String, String> kafkaProducer;
	private final MqttConnectOptions options = new MqttConnectOptions();

	private void connect(String serverURI,
						 String clientId,
						 String zkConnect,
						 String caCrt,
						 String clientCrt,
						 String clientKey,
						 String password,
						 String username,
						 String userPass
	) throws Exception {

		mqtt = new MqttClient(serverURI, clientId);
		mqtt.setCallback(this);
		Properties props = new Properties();

		if(username != null && !username.isEmpty()){
			options.setUserName(username);
		}
		if(userPass != null && !userPass.isEmpty()){
			options.setPassword(userPass.toCharArray());
		}
		mqtt.connect(options);
		mqtt.setCallback(this);
		props.put("metadata.broker.list", zkConnect + ":9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		kafkaProducer = new Producer<>(config);
		logger.info("Connected to MQTT and Kafka");
	}

	private void reconnect() throws MqttException {
		IMqttToken token = mqtt.connectWithResult(options);
		token.waitForCompletion();
		System.out.println("MQTT Reconnected! at: " + LocalTime.now() );
	}
	
	private void subscribe(String[] mqttTopicFilters) throws MqttException {
		int[] qos = new int[mqttTopicFilters.length];
		mqtt.subscribe(mqttTopicFilters, qos);
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.warn("Lost connection to MQTT server", cause);
		while (true) {
			try {
				logger.info("Attempting to reconnect to MQTT server");
				reconnect();
				logger.info("Reconnected to MQTT server, resuming");
				return;
			} catch (MqttException e) {
				logger.warn("Reconnect failed, retrying in 10 seconds", e);
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {

	}

	@Override
	public void messageArrived(String topic, MqttMessage message){
		
		byte[] payload = message.getPayload();
		logger.info("Message: " + new String(payload) + " on topic: " + topic + " timestamp: " + LocalTime.now());
		KeyedMessage<String, String> data = new KeyedMessage<>(topic, new String(payload));
		kafkaProducer.send(data);
	}

	/**
	 * @param args arguments
	 */
	public static void main(String[] args) {
		CommandLineParser parser = null;
		try {
			parser = new CommandLineParser();
			parser.parse(args);
			Bridge bridge = new Bridge();
			bridge.connect(parser.getServerURI(), parser.getClientId(), parser.getZkConnect(), parser.getCaCrt(),
				parser.getClientCrt(), parser.getClientKey(), parser.getPassword(), parser.getUsername(),parser.getUserPass());
			bridge.subscribe(parser.getMqttTopicFilters());
		} catch (MqttException e) {
			e.printStackTrace(System.err);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}