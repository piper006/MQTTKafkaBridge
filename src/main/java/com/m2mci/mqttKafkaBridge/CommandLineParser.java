package com.m2mci.mqttKafkaBridge;

import java.io.OutputStream;
import java.io.PrintStream;

import lombok.Data;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

@Data
public class CommandLineParser {
	private static final String ALL_MQTT_TOPICS = "#";
	private static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost";
	private static final String DEFAULT_MQTT_SERVER_URI = "tcp://localhost:1883";

	@Option(name="--ca", usage="MQTT CA.CRT")
	private String caCrt = "./ca.crt";

	@Option(name="--clientCrt", usage="MQTT CLIENT.CRT")
	private String clientCrt = "./client.crt";

	@Option(name="--clientKey", usage="MQTT CLIENT.KEY")
	private String clientKey = "./client.key";

	@Option(name="--certPass", usage="MQTT CERT PASSWORD")
	private String password = "";

	@Option(name="--id", usage="MQTT Client ID")
	private String clientId = "mqttKafkaBridge";

	@Option(name="--uri", usage="MQTT Server URI")
	private String serverURI = DEFAULT_MQTT_SERVER_URI;

	@Option(name="--zk", usage="Zookeeper connect string")
	private String zkConnect = DEFAULT_ZOOKEEPER_CONNECT;

	@Option(name="--topics", usage="MQTT topic filters (comma-separated)")
	private String mqttTopicFilters = ALL_MQTT_TOPICS;

	@Option(name="--help", aliases="-h", usage="Show help")
	private boolean showHelp = false;

	private CmdLineParser parser = new CmdLineParser(this);

	public String getClientId() {
		return clientId;
	}

	public String getServerURI() {
		return serverURI;
	}

	public String getZkConnect() {
		return zkConnect;
	}

	public String[] getMqttTopicFilters() {
		return mqttTopicFilters.split(",");
	}

	public void parse(String[] args) throws CmdLineException {
		parser.parseArgument(args);
		if (showHelp) {
			printUsage(System.out);
			System.exit(0);
		}
	}

	public void printUsage(OutputStream out) {
		PrintStream stream = new PrintStream(out);
		stream.println("java " + Bridge.class.getName() + " [options...]");
		parser.printUsage(out);
	}
}
