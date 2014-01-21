package spanner.node;

import org.zeromq.ZMQ;

public class TestClient {
	
	
	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);


		// Socket to send messages to
		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.connect("tcp://127.0.0.1:20001");
		//sender.bind("tcp://localhost:10001");
		sender.send("hi", 0);
		
	}

}
