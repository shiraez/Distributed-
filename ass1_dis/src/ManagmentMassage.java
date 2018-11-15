import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.Map;

public class ManagmentMassage {

    public static String createQueueManToApp(AmazonSQS sqs) {
        CreateQueueRequest createQueueRequestManToApp = new CreateQueueRequest("ManToApp");
        return sqs.createQueue(createQueueRequestManToApp).getQueueUrl();


    }
    public static boolean reciveMessage(String myQueueUrlManToApp, AmazonSQS sqs) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrlManToApp);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        myWait();
        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
            String messageRecieptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrlManToApp, messageRecieptHandle));
            return true;
        }
        return false;
    }

    private static void myWait() {
    }
}
