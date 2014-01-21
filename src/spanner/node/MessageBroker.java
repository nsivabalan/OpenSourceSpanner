package spanner.node;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.Poller;

public class MessageBroker implements Runnable{

	Context context = null;
	String serverAddress = null;
	String clientAddress = null;
	Socket frontend = null;
	Socket backend = null;
	Poller items = null;
	public MessageBroker(String host1, String host2) {
		
		
		// Prepare our context and sockets
		context = ZMQ.context(1);
		serverAddress = getAddress(host1);
		clientAddress = getAddress(host2);
		frontend = context.socket(ZMQ.ROUTER);
		backend = context.socket(ZMQ.DEALER);
		frontend.bind(clientAddress);
		backend.bind(serverAddress);

		System.out.println("launch and connect broker.");

		// Initialize poll set
		items = new Poller (2);
		items.register(frontend, Poller.POLLIN);
		items.register(backend, Poller.POLLIN);

	}
	
	public void run(){	
		
		boolean more = false;
		byte[] message;

		// Switch messages between sockets
		while (!Thread.currentThread().isInterrupted()) {
			// poll and memorize multipart detection
			items.poll();

			if (items.pollin(0)) {
				while (true) {
					// receive message
					message = frontend.recv(0);
					more = frontend.hasReceiveMore();

					// Broker it
					backend.send(message, more ? ZMQ.SNDMORE : 0);
					if(!more){
						break;
					}
				}
			}
			if (items.pollin(1)) {
				while (true) {
					// receive message
					message = backend.recv(0);
					more = backend.hasReceiveMore();
					// Broker it
					frontend.send(message, more ? ZMQ.SNDMORE : 0);
					if(!more){
						break;
					}
				}
			}
		}
		// We never get here but clean up anyhow
		frontend.close();
		backend.close();
		context.term();
	}
	
	private String getAddress(String hostaddress)
	{
			String[] hostDetails = hostaddress.split(":");
			return new String("tcp://"+hostDetails[0]+":"+hostDetails[1]);	
	}
	
}