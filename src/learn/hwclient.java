package learn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.zeromq.ZMQ;

public class hwclient{

	public void sendMsg(String host, int port) throws IOException
	{
		ZMQ.Context context = ZMQ.context(1);

		System.out.println("Connecting to Server");

		ZMQ.Socket socket = context.socket(ZMQ.REQ);
		socket.connect(getAddress(host, port));
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		for(int requestNbr = 0; requestNbr != 10; requestNbr++) {
			System.out.println("Enter string to send to server");
			String request = br.readLine();
			Long startTime = System.currentTimeMillis();
			System.out.println("Sending " + request );
			socket.send(request.getBytes (), 0);
			byte[] reply = socket.recv(0);
			System.out.println("Received response in "+(System.currentTimeMillis() - startTime));
			System.out.println("Received " + new String (reply));
		}

		socket.close();
		context.term();
	}
	
	public String getAddress(String host, int port)
	{
		return new String("tcp://"+host+":"+port);
	}
	
	
	public static void main (String[] args) throws IOException{
		
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		
		hwclient obj = new hwclient();
		obj.sendMsg(host, port);
	}
}