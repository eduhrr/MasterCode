import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

public class serverWorkerThread implements Runnable {
	private Socket serverSocket;
	private String url;
	private String receipHandle;
	private int rowID;

	public serverWorkerThread(Socket socket, String url) throws SocketException {
		this.serverSocket = socket;
		this.serverSocket.setSoTimeout(60000); // TODO: 20 mins ->control
												// infinite loop case in
												// LunaWorker
		this.url = url;

	}

	@Override
	public void run() {
		DataInputStream input;
		DataOutputStream output;

		try {
			input = new DataInputStream(serverSocket.getInputStream());
			output = new DataOutputStream(serverSocket.getOutputStream());

			// listen the rowID
			setRowID(Integer.valueOf(input.readUTF()));
			System.out.println("rowID=" + getRowID());
			
			// listen the receiptHandle
			setReceipHandle(input.readUTF());
			System.out.println("receipHandle=" + getReceipHandle());

			// change visibility
			ChangeMessageVisibilityRequest changeVisibility = new ChangeMessageVisibilityRequest(
					url, this.receipHandle, 60 * 60 * 10); // 10 h in seconds
															// max 12h
			readQueue.getSqs().changeMessageVisibility(changeVisibility);

			// send the rowID
			// output.writeInt(312);

			// listen the worker status
			String result = "";
			do {
				result = input.readUTF();
				System.out.println("result=" + result);
				if (result.equals("ERROR")) {
					// error --> terminate visibility timeout
					changeVisibility.setVisibilityTimeout(0);
					readQueue.getSqs()
							.changeMessageVisibility(changeVisibility);
				}
			} while (!result.equals("The rendering has been finished"));

			DeleteMessageRequest delRequest = new DeleteMessageRequest(url,
					this.receipHandle);
			readQueue.getSqs().deleteMessage(delRequest);

			output.close();
			input.close();
			serverSocket.close();

		} catch (IOException e) {
			// bad result --> terminate visibility timeout
			ChangeMessageVisibilityRequest changeVisibility = new ChangeMessageVisibilityRequest(
					url, this.receipHandle, 0);
			readQueue.getSqs().changeMessageVisibility(changeVisibility);

			e.printStackTrace();
		}
	}

	public Socket getServerSocket() {
		return serverSocket;
	}

	public void setServerSocket(Socket serverSocket) {
		this.serverSocket = serverSocket;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getReceipHandle() {
		return receipHandle;
	}

	public void setReceipHandle(String receipHandle) {
		this.receipHandle = receipHandle;
	}

	public int getRowID() {
		return rowID;
	}

	public void setRowID(int rowID) {
		this.rowID = rowID;
	}
	

}
