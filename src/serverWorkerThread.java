import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

public class serverWorkerThread implements Runnable {
	private Socket serverSocket;
	private String url;
	private String receipHandle;
	private int rowID;

	public serverWorkerThread(Socket socket, String url) throws SocketException {
		setServerSocket(socket);
		// Giving 15 mins max to receive the rowID and then other 15 to receive
		// the receiptHandle before the this thread will finish.
		getServerSocket().setSoTimeout(15 * 60 * 1000);
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

			// Giving 1 hour to the worker to send the status before this thread
			// will be closed
			// getServerSocket().setSoTimeout(1 * 60 * 60 * 1000);
			// TODO: testing
			getServerSocket().setSoTimeout(60 * 1000); // 1 minute test

			// changing visibility to 11 hours
			ChangeMessageVisibilityRequest changeVisibility = new ChangeMessageVisibilityRequest(
					url, this.receipHandle, 11 * 60 * 60);
			readQueue.getSqs().changeMessageVisibility(changeVisibility);

			// send the rowID
			// output.writeInt(312);

			// listen the worker status
			String result = "";
			do {
				result = input.readUTF();
				System.out.println("Worker " + getRowID() + "= " + result);
				if (result.equals("ERROR")) {
					// error --> terminate visibility timeout
					changeVisibility.setVisibilityTimeout(0);
					readQueue.getSqs()
							.changeMessageVisibility(changeVisibility);
				}
			} while (!result.equals("The rendering has been finished"));

			System.out.println("Deleting SQS message with rowID=" + getRowID());
			DeleteMessageRequest delRequest = new DeleteMessageRequest(url,
					this.receipHandle);
			readQueue.getSqs().deleteMessage(delRequest);

			output.close();
			input.close();
			serverSocket.close();
			System.out.println("communication thread with Worker " + getRowID()
					+ "is ending");
		} catch (SocketTimeoutException e) {
			System.out.println("Communication Timeout with the worker "
					+ getRowID() + " has elapsed");
			System.out.println("The rendering work " + getRowID()
					+ " will need to be restarted");
			ChangeMessageVisibilityRequest changeVisibility = new ChangeMessageVisibilityRequest(
					url, this.receipHandle, 0);
			readQueue.getSqs().changeMessageVisibility(changeVisibility);

			e.printStackTrace();
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
