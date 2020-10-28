package pt.uminho.iata;

import org.eclipse.paho.client.mqttv3.MqttException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Command line utility for publishing and subscribing to Adafruit sensor feed
 */
@Command(name = "ada", mixinStandardHelpOptions = true, version = "ada 1.0",
		description = "Publishes and subscribes to Adafruit.io topics (for IATA)")
public class IataAdafruitCli implements Runnable
{
	private static final String ADAFRUIT_USERNAME = "pg42819";
	private static final String ADAFRUIT_AIO_KEY = "REMOVED_FOR_GITHUB_PUBLISH";

	private static final int DEFAULT_QoS = 1;
	private static final int DEFAULT_SLEEP = 0; // seconds or -1 for indefinite
	private static final String ADAFRUIT_BROKER_URI = "tcp://io.adafruit.com:1883"; //Adafruit IO broker

	public static final String DEFAULT_SUBSCRIBE_CLIENT_ID = "IATA-Subscribe-Client";
	public static final String DEFAULT_PUBLISH_CLIENT_ID = "IATA-Publish-Client";
	public static final String DEFAULT_TOPIC = "feeds/sensorfeed";

	@Option(names = { "-t", "--topic"}, description = "Topic to which to publish or subscribe")
	private String _topic = DEFAULT_TOPIC;

	@Option(names = { "-m", "--message"}, description = "Message to publish")
	private String _message;

	@Option(names = { "-s", "--subscribe"}, description = "Subscribe and listen for messages and output to the console")
	private boolean _listen = false;

	@Option(names = { "-q", "--qos"},
			description = "Quality of Service: 0 = at most once, 1 = at least once, 2 = exactly once. "
						  + "(Defaults to ${DEFAULT-VALUE})")
	private int _qos = DEFAULT_QoS;

	@Option(names = { "-f", "--file" }, description = "File to which subscribed messages are logged")
	private String _filePath;

	@Option(names = {"--timeout"},
			description = "Subscription timeout in seconds. Use 0 to wait forever (Defaults to ${DEFAULT-VALUE})")
	private int _sleepTimeout = DEFAULT_SLEEP;

	@Option(names = { "--client-id"},
			description = "Id to use for the client. (Defaults to fixed values for subscribe and publish)")
	private String _clientId = null;

	@Option(names = {"--aio-user"}, description = "Adafruit.IO username. (Defaults to ${DEFAULT-VALUE})")
	private String _aioUser = ADAFRUIT_USERNAME;

	@Option(names = {
			"--aio-key"}, description = "Adafruit.IO API key. Defaults to AIO_KEY env var or fails if not supplied")
	private String _aioKey;

	@Option(names = {"--aio-broker"}, description = "Adafruit.IO Broker URI. (Defaults to ${DEFAULT-VALUE})")
	private String _aioBroker = ADAFRUIT_BROKER_URI;

	// inject the picocli spec for use in error handling
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec _picospec;

	public static void main(String...args)
	{
		int exitCode = new CommandLine(new IataAdafruitCli()).execute(args);
		System.exit(exitCode);
	}

	@Override
	public void run()
	{
		String aioKey = _aioKey != null ? _aioKey : System.getenv("AIO_KEY");
		if (aioKey == null) {
			throw new CommandLine.ParameterException(_picospec.commandLine(),
				"Cannot continue without an Adafruit.IO API Key. Pass one with --aio-key or set the env var AIO_KEY");
		}

		IataAdafruitClient adafruitClient = new IataAdafruitClient(_aioBroker, _aioUser, aioKey);

		try {
			if (_listen) {
				String clientId = _clientId != null ? _clientId : DEFAULT_SUBSCRIBE_CLIENT_ID;

				try (IataAdafruitClient connection = adafruitClient.connect(clientId)) {
					PrintWriter writer;
					// write to stdout if there is no file specified
					if (_filePath == null) {
						writer = new PrintWriter(System.out);
					}
					else {
						writer = new PrintWriter(new FileWriter(_filePath, true));
					}
					System.out.println("Subscribed events written to " + _filePath);
					// loop until ctrl-c (lame but demo only)
					connection.subscribe(_topic, writer, _qos, _sleepTimeout);
				}
				catch (MqttException me) {
					IataAdafruitClient.logError(me, "Problem with Adafruit subscription");
				}
			}
			else {
				String clientId = _clientId != null ? _clientId : DEFAULT_PUBLISH_CLIENT_ID;

				String msg;
				if (_message == null) {
					System.out.println("No message was specified with --message, publishing a test message");
					msg = "test message";
				}
				else {
					msg = _message;
				}
				try (IataAdafruitClient connection = adafruitClient.connect(clientId)) {
					connection.publish(_topic, msg, _qos);
				}
				catch (MqttException me) {
					IataAdafruitClient.logError(me, "Problem with Adafruit publish");
				}
			}
		}
		catch (Exception e){
			// catching for autoclosable: should not be necessary
			throw new RuntimeException(e);
		}
	}
}
