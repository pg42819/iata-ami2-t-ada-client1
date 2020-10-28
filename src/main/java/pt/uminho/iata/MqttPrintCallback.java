package pt.uminho.iata;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.PrintWriter;

/**
 * Implements an MqttCallback to print incoming messages to a print writer (usually a file)
 */
public class MqttPrintCallback implements MqttCallback
{
	private PrintWriter _out;

	public MqttPrintCallback(PrintWriter out)
	{
		_out = out;
	}

	@Override
	public void connectionLost(Throwable cause)
	{
		_out.println("Connection Lost: " + cause.getLocalizedMessage());
		cause.printStackTrace(_out);
		_out.close();
	}

	@Override
	public void messageArrived(String topic, MqttMessage message)
	{
		final byte[] payload = message.getPayload();
		String ouputMessage =
				String.format("--- Incoming message [%s] for topic [%s] : [%s]\n",
							  message.getId(), topic, new String(payload));
		_out.println(ouputMessage);
		_out.flush();
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token)
	{
		_out.println("Delivery complete");
		_out.flush();
	}
}

