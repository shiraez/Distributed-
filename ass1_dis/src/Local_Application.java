import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import java.io.BufferedReader;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import org.apache.commons.codec.binary.Base64;

public class Local_Application {
	static AWSCredentialsProvider credentialsProvider;
	static AmazonEC2 ec2;
	static AmazonS3 s3;
	static AmazonSQS sqs;
	static String file_nameURL;
	static String bucketName;
	static boolean ManegerActive = false;
	static List<Instance> instanceManager;
	static String key;
	static String myQueueUrlManToApp;
	static String addToQueueName = "";
	
	
	public static void main(String[] args) throws Exception {
        init();
        CreateManager();
        CreateDirectory(args[0]);
        UploadToS3();
        sentMassegeToManager(Integer.parseInt(args[1]));
        while(!reciveMessage())
        	myWait();
        downloadsSummary();
        CreatesHtml(); 
        
    }
	
	private static void CreatesHtml() {
		// TODO Auto-generated method stub
		
	}

	public static void init() {
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
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
	public static boolean ManegerActive() {
		return false;
	}
	
	
	public static void CreateManager() {
		 try {
				 if(!ManegerActive()) {

                     IamInstanceProfileSpecification instanceP = new IamInstanceProfileSpecification();
                     instanceP.setArn("arn:aws:iam::683725846471:instance-profile/ManagerRole");
		            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
		            RunInstancesRequest request = new RunInstancesRequest("ami-76f0061f", 1, 1);
		            request.setInstanceType(InstanceType.T2Nano.toString()); //hard ware
                    request.setUserData(getManagerUserData());
                    request.withSecurityGroups("kerenshira");
                    request.withKeyName("theQueen");
                    request.setIamInstanceProfile(instanceP);
                    instanceManager = ec2.runInstances(request).getReservation().getInstances();
		            System.out.println("Launch instances: " + instanceManager);
		            ManegerActive = true;
				 }
		 
	        } catch (AmazonServiceException ase) {
	            System.out.println("Caught Exception: " + ase.getMessage());
	            System.out.println("Reponse Status Code: " + ase.getStatusCode());
	            System.out.println("Error Code: " + ase.getErrorCode());
	            System.out.println("Request ID: " + ase.getRequestId());
	        }
		 
	}

	public static String getManagerUserData(){
        StringBuilder managerBuild = new StringBuilder();
        managerBuild.append("#!/bin/bash\n");
        managerBuild.append("sudo su\n");
        managerBuild.append("yum -y install java-1.8.0 \n");
        managerBuild.append("alternatives --remove java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n");
        managerBuild.append("aws s3 cp s3://"+bucketName+"/manager.zip  manager.zip\n");
        managerBuild.append("unzip manager.zip\n");
        managerBuild.append("java -jar manager.jar\n");

        return new String(Base64.encodeBase64(managerBuild.toString().getBytes()));
    }
	
	public static void CreateDirectory(String file_dir) {        
        bucketName =
                credentialsProvider.getCredentials().getAWSAccessKeyId();
        
        try {

            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);
 
            /*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}
	
	public static String UploadToS3() {
		System.out.println("Uploading a new object to S3 from a file\n");
        File file = new File(file_nameURL);
        key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
        PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
        s3.putObject(req);
        
        return key;
	}
	
	public static void sentMassegeToManager(int numOfImgesForWorker) {
        CreateQueueRequest createQueueRequestAppToMang = new CreateQueueRequest("AppToMan"+ addToQueueName);
        String myQueueUrlAppToMang = sqs.createQueue(createQueueRequestAppToMang).getQueueUrl();
        createQueueManToApp();
        sqs.sendMessage(new SendMessageRequest(myQueueUrlAppToMang, "new task " + numOfImgesForWorker+"_"+key));

	}
	
	public static void createQueueManToApp() { 
        CreateQueueRequest createQueueRequestManToApp = new CreateQueueRequest("ManToApp" + addToQueueName);
        myQueueUrlManToApp = sqs.createQueue(createQueueRequestManToApp).getQueueUrl();
        
       
	}
	public static boolean reciveMessage() {
		 ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrlManToApp);
         List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
         myWait();
         for (Message message : messages) {
             if (message.getBody().startsWith("done task answer")) {
                 downloadsSummary();
                 for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                     System.out.println("  Attribute");
                     System.out.println("    Name:  " + entry.getKey());
                     System.out.println("    Value: " + entry.getValue());
                 }
                 String messageRecieptHandle = message.getReceiptHandle();
                 sqs.deleteMessage(new DeleteMessageRequest(myQueueUrlManToApp, messageRecieptHandle));
                 return true;
             }
         }
         return false;
	}
	
	public static void downloadsSummary() {
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        createHtmlFile(reader);
	}

    private static void createHtmlFile(BufferedReader reader) {
//        File htmlTemplateFile = new File("path/template.html");
//        String htmlString = FileUtils.readFileToString(htmlTemplateFile);
//        String title = "New Page";
//        String body = "This is Body";
//        htmlString = htmlString.replace("$title", title);
//        htmlString = htmlString.replace("$body", body);
//        File newHtmlFile = new File("path/new.html");
//        FileUtils.writeStringToFile(newHtmlFile, htmlString);
    }


    private static void readFileFromS3(S3ObjectInputStream objectContent) {
    }

    private static void myWait() {
		// TODO Auto-generated method stub
		
	}

	
}
