import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import java.io.BufferedReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
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

public class Manager {
	static AWSCredentialsProvider credentialsProvider;
	static AmazonEC2 ec2;
	static AmazonS3 s3;
	static AmazonSQS sqs;
	static String myQueueUrlManToApp;
	static String myQueueUrlAppToMan;
	static String bucketName;
	static String key;
	private static String myQueueUrlManToWorker;
	private static String myQueueUrlWorkerToMan;

	public static void main(String[] args) throws Exception {
        init();
        downloadsImage();
        CreatesSQSForURL();
        BootstrapsWorker();
        readMessages();
        createSummary();        
        UploadFileToS3();
      
         
        
    }
	
	private static void UploadFileToS3() {
		// TODO Auto-generated method stub
		
	}

	private static boolean readMessages() {
		// TODO Auto-generated method stub
		return false;
	}
	
	private static boolean createSummary() {
		// TODO Auto-generated method stub
		return false;
	}

	private static void BootstrapsWorker() {
		// TODO Auto-generated method stub
		
	}


	private static void CreatesSQSForURL() {
		// TODO Auto-generated method stub
		
	}

	private static void downloadsImage() {
		System.out.println("Downloading an object");
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
		System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
		displayTextInputStream(object.getObjectContent());
		
	}

	public static void createQueueManToApp() {
		CreateQueueRequest createQueueRequestManToApp = new CreateQueueRequest("ManToApp");
		myQueueUrlManToApp =  sqs.createQueue(createQueueRequestManToApp).getQueueUrl();

	}

	public static void createQueueAppToMan() {
		CreateQueueRequest createQueueRequestAppToMan = new CreateQueueRequest("AppToMan");
		myQueueUrlAppToMan = sqs.createQueue(createQueueRequestAppToMan).getQueueUrl();

	}

	private static void createQueueWorkerToMan() {
		CreateQueueRequest createQueueRequestWorkerToMan = new CreateQueueRequest("ManToApp");
		myQueueUrlWorkerToMan =  sqs.createQueue(createQueueRequestWorkerToMan).getQueueUrl();
	}

	private static void createQueueManToWorker() {
		CreateQueueRequest createQueueRequestManToWorker = new CreateQueueRequest("ManToApp");
		myQueueUrlManToWorker =  sqs.createQueue(createQueueRequestManToWorker).getQueueUrl();
	}

	public static boolean reciveMessage() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrlAppToMan);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		String pattern = "new task (\\d*)_(.*)";
		Pattern r = Pattern.compile(pattern);

		myWait();
		for (Message message : messages) {
			Matcher m = r.matcher(message.getBody());
			int numOfImagesPerWorker = Integer.parseInt(m.group(0));
			key = m.group(1);
			//TODO add Thread
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(myQueueUrlAppToMan, messageRecieptHandle));
			return true;
		}
		return false;
	}

	private static void myWait() {
	}

	private static void displayTextInputStream(InputStream input) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = "";
		int numWorker = 0;
		try {
			while ((line = reader.readLine()) != null) {
//				if()
			}
			System.out.println();
		}
		catch (Exception e){}
	}


	private static void init() {
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

		bucketName =
				credentialsProvider.getCredentials().getAWSAccessKeyId();
		createQueueManToApp();
		createQueueAppToMan();
		createQueueManToWorker();
		createQueueWorkerToMan();

	}



}
