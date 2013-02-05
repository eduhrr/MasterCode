import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;

public class SendSQS{

   public static void main(String[] args) throws Exception{
      AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(SendSQS.class.getResourceAsStream("AwsCredentials.properties")));

      try{
         GetQueueUrlRequest qrequest = new GetQueueUrlRequest("iitLuna");
         String url = sqs.getQueueUrl(qrequest).getQueueUrl();

         //sqs.sendMessage(new SendMessageRequest(url, "55,ts"));
         sqs.sendMessage(new SendMessageRequest(url, "55"));
      }catch (AmazonServiceException ase) {
         System.out.println("Error Message:    " + ase.getMessage());
         System.out.println("HTTP Status Code: " + ase.getStatusCode());
         System.out.println("AWS Error Code:   " + ase.getErrorCode());
         System.out.println("Error Type:       " + ase.getErrorType());
         System.out.println("Request ID:       " + ase.getRequestId());
      } catch (AmazonClientException ace) {
         System.out.println("Error Message: " + ace.getMessage());
      } 
   }

}
