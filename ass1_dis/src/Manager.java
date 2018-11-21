import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Manager {
	static AWSCredentialsProvider credentialsProvider;
	static AmazonEC2 ec2;
	static AmazonS3 s3;
	static AmazonSQS sqs;
	static String myQueueUrlManToApp;
	static String myQueueUrlAppToMan;
	static String bucketName;
	static String key = "";
	private static String myQueueUrlManToWorker;
	private static String myQueueUrlWorkerToMan;
    private static String addToQueueName = "";
    private static List<TaskManager> tasks;

    public static void main(String[] args) throws Exception {
        init();
        
    }
	



	public static void createQueueAppToMan() {
		CreateQueueRequest createQueueRequestAppToMan = new CreateQueueRequest("AppToMan" +addToQueueName);
		myQueueUrlAppToMan = sqs.createQueue(createQueueRequestAppToMan).getQueueUrl();

	}

    private static void createQueueManToWorker() {
        CreateQueueRequest createQueueRequestManToWorker = new CreateQueueRequest("ManToWorker" + addToQueueName);
        myQueueUrlManToWorker =  sqs.createQueue(createQueueRequestManToWorker).getQueueUrl();
    }

    public static void createQueueManToApp() {
        CreateQueueRequest createQueueRequestManToApp = new CreateQueueRequest("ManToApp" + addToQueueName);
        myQueueUrlManToApp =  sqs.createQueue(createQueueRequestManToApp).getQueueUrl();

    }

    private static void createQueueWorkerToMan() {
        CreateQueueRequest createQueueRequestWorkerToMan = new CreateQueueRequest("WorkerToMan" + addToQueueName);
        myQueueUrlWorkerToMan =  sqs.createQueue(createQueueRequestWorkerToMan).getQueueUrl();
    }





	public static boolean reciveMessageFromLocal() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrlAppToMan);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		String pattern = "new task (\\d*)_(.*)";
		Pattern r = Pattern.compile(pattern);

		myWait();
		for (Message message : messages) {
			Matcher m = r.matcher(message.getBody());
            m.matches();
			int numOfImagesPerWorker = Integer.parseInt(m.group(1));
			System.out.println("numOfImagesPerWorker: " + numOfImagesPerWorker);
			key = m.group(2);
			System.out.println("key: " + key);
			TaskManager new_task = new TaskManager(ec2, s3, sqs, bucketName, key, numOfImagesPerWorker, myQueueUrlManToWorker, myQueueUrlWorkerToMan, myQueueUrlManToApp);
			Thread task =new Thread(new_task);
			task.start();
            tasks.add(new_task);
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(myQueueUrlAppToMan, messageRecieptHandle));
			return true;
		}
		return false;
	}

	private static void myWait() {
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
		createQueueAppToMan();
        createQueueManToWorker();
        createQueueManToApp();
        createQueueWorkerToMan();

		while(true){
			reciveMessageFromLocal();

			if(allTaskEnd()){
			    killWorker();
            }

		}

    }

    public static boolean allTaskEnd(){
        for(TaskManager task:tasks){
            if(!task.endTask){
                return false;
            }
        }
        return true;

    }


    private static void killWorker() {
        for (TaskManager task: tasks){
            List<String> list_ins = task.workers.stream().map(p->p.getInstanceId()).collect(Collectors.toList());
            TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(list_ins);
                ec2.terminateInstances(terminateRequest);
        }
        tasks.clear();
    }


}
