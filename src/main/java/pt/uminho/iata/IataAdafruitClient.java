package pt.uminho.iata;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.PrintWriter;

/**
 * Wrapper around an Mqtt client for an Adafruit feed
 */
public class IataAdafruitClient implements AutoCloseable
{

	private final String _brokerUri;
	private final String _aioUser;
	private final String _aioKey;
	private MqttClient _mqttClient = null;

	public IataAdafruitClient(String brokerUri, String aioUser, String aioKey)
	{
		_brokerUri = brokerUri;
		_aioUser = aioUser;
		_aioKey = aioKey;
	}

	public IataAdafruitClient connect(String clientId) throws MqttException
	{
		MemoryPersistence persistence = new MemoryPersistence();
		_mqttClient = new MqttClient(_brokerUri, clientId, persistence);
		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setConnectionTimeout(60);
		connOpts.setKeepAliveInterval(60);
		connOpts.setCleanSession(true);
		connOpts.setUserName(_aioUser);
		connOpts.setPassword(_aioKey.toCharArray());
		_info("Connecting to broker: %s with client id: %s", _brokerUri, clientId);
		_mqttClient.connect(connOpts);
		_info("Successfully connected to Adafruit.io via MQTT");
		return this;
	}

	public void publish(String topic, String content, int qos) throws MqttException
	{
		String fullTopic = _fullTopic(topic);
		_info("Publishing message '%s' to topic '%s' with qos %d", content, fullTopic, qos);
		MqttMessage message = new MqttMessage(content.getBytes());
		message.setQos(qos);
		_getClient().publish(fullTopic, message);
		System.out.println("Message published");
	}

	public void subscribe(String topic, PrintWriter output, int qos, int wait) throws MqttException, InterruptedException
	{
		String fullTopic = _fullTopic(topic);
		_info("Subscribing to topic '%s' with qos %d", fullTopic, qos);
		_getClient().setCallback(new MqttPrintCallback(output));
		_getClient().subscribe(fullTopic, qos);
		String waitTime = wait == 0 ? "indefinitely" : String.format("for %d seconds", wait);
		_info("Subscribed to topic '%s' and waiting for messages %s", fullTopic, waitTime);
		if (wait == 0) {
			while (true) {
				Thread.sleep(500);
			}
		}
		else {
			Thread.sleep(wait * 1000);
		}
	}

	private String _fullTopic(String topic)
	{
		return String.format("%s/%s", _aioUser, topic);
	}

	private MqttClient _getClient()
	{
		if (_mqttClient == null) {
			throw new IllegalStateException("Attempted to use the Adafruit client before connecting.");
		}
		return _mqttClient;
	}

	private void _info(String msg, Object... params)
	{
		// TODO add real logging at some point
		System.out.printf("[INFO] " + msg + "\n", params);
	}

	private void _error(String msg, Object... params)
	{
		// TODO add real logging at some point
		System.err.printf("[ERROR] " + msg + "\n", params);
	}

	public static void logError(MqttException me, String msg, Object... params)
	{
		String message = msg == null ? me.getLocalizedMessage() :
						 String.format(msg, params);

		System.err.printf("%s\nMQTT Reason-code: %d\nException: %s\n",
						  message, me.getReasonCode(), me.getLocalizedMessage());
		me.printStackTrace();
	}

	@Override
	public void close() throws Exception
	{
		if (_mqttClient != null) {
			_mqttClient.disconnect();
			_info("Disconnected from the adatafruit client: %s", _mqttClient.getClientId());
		}
	}
}
