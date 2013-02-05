import java.net.*;
import java.io.*;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;

public class MasterSocket extends Thread {
	private ServerSocket serverSocket;
	private String url;


	public MasterSocket(int port, String url) throws IOException {
		this.serverSocket = new ServerSocket(port);
		//serverSocket.setSoTimeout(100000); // 600000 10 min
		this.url = url;

	}

	public void run() {
		try {
			while (true){
				Socket workerSocket  = serverSocket.accept();
				Runnable newWorker = new serverWorkerThread(workerSocket, url); 
		        Thread t = new Thread(newWorker);
		        t.start();  
			}
		} catch (IOException e) {
			//something bad --> terminate visibility timeout
			//TODO new ChangeMessageVisibilityRequest(url,messHandle,0);
			e.printStackTrace();
		}
	}
}


//	public static void main(String[] args) {
//		try {
//			new MasterSocket(6060).start();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//}