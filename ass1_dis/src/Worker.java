import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
//import net.sourceforge.javaocr.ocrPlugins.mseOCR.OCRScanner;
//import net.sourceforge.javaocr.scanner.accuracy.OCRComp;
//import net.sourceforge.javaocr.scanner.accuracy.OCRIdentification;


public class Worker {
    static AWSCredentialsProvider credentialsProvider;
    static AmazonEC2 ec2;
    static AmazonS3 s3;
    static AmazonSQS sqs;

    //URL's
    static String queueWorkerToManager;
    static String queueManagerToWorker;
    //-------------------to check!!!-------------------------
    static String addToQueueName = "" + UUID.randomUUID();

    static List<Message> messagesFromManager;

    public static void main(String[] args){
        init();
        CreatesSQSqueue();
        while(true) {
            getMessageFromManager();
        }

    }


    public static void init(){
        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-west-2").build();
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
    }

    public static void CreatesSQSqueue() {
        CreateQueueRequest createQueueRequestWorkerToManager = new CreateQueueRequest("WorkerToMan"+ addToQueueName);
        queueWorkerToManager = sqs.createQueue(createQueueRequestWorkerToManager).getQueueUrl();
        createQueueRequestWorkerToManager = new CreateQueueRequest("ManToWorker"+ addToQueueName);
        queueManagerToWorker = sqs.createQueue(createQueueRequestWorkerToManager).getQueueUrl();
    }

    //worker gets a task from manager.
    public static void getMessageFromManager() {
        //create receive message object from url queue.
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueManagerToWorker);
        messagesFromManager = sqs.receiveMessage(receiveMessageRequest).getMessages();
        //if get some message.
        if(messagesFromManager.size()>0)
            applyOCR();
    }



    //get url msg.
    //delete msg from queues.

    public static void applyOCR() {
        //foreach message that received from manager.
        for (Message message: messagesFromManager){
            String imageURL = message.getBody();
            //delete message after receipt
            sqs.deleteMessage(queueManagerToWorker, message.getReceiptHandle());
            messagesFromManager.remove(message);
/*
            Image image = new Image();
            image.
            OCRScanner ocr = new OCRScanner();
            ocr.
  */
            //OCR

String txt = "";


            sendMessageToManager(imageURL, txt);

        }
    }

    public static void sendMessageToManager(String imageURL, String txt) {
        sqs.sendMessage(new SendMessageRequest(queueWorkerToManager, "done image task " + imageURL +","+ txt));
    }

}
