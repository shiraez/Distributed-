import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.codec.binary.Base64;

public class TaskManager implements Runnable{
    static AmazonEC2 ec2;
    static AmazonS3 s3;
    static AmazonSQS sqs;
    static String myQueueUrlManToApp;
    static String myQueueUrlAppToMan;
    static String bucketName;
    static String key = "";
    static int numOfTaskPerWorker;
    static List<Instance> workers;
    private static String myQueueUrlManToWorker;
    private static String myQueueUrlWorkerToMan;
    static int numberOfURLS;

    public TaskManager(AmazonEC2 ec2, AmazonS3 s3, AmazonSQS sqs, String bucketName, String key, int numOfTaskPerWorker) {
        this.ec2 = ec2;
        this.s3 = s3;
        this.sqs = sqs;
        this.bucketName = bucketName;
        this.key = key;
        this.numOfTaskPerWorker = numOfTaskPerWorker;

    }



    private static void createQueueManToWorker() {
        CreateQueueRequest createQueueRequestManToWorker = new CreateQueueRequest("ManToApp");
        myQueueUrlManToWorker =  sqs.createQueue(createQueueRequestManToWorker).getQueueUrl();
    }

    public static void createQueueManToApp() {
        CreateQueueRequest createQueueRequestManToApp = new CreateQueueRequest("ManToApp");
        myQueueUrlManToApp =  sqs.createQueue(createQueueRequestManToApp).getQueueUrl();

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



    private static void downloadsImage() {
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        readFileFromS3(object.getObjectContent());

    }

    private static void readFileFromS3(InputStream input) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = "";
        int numOfTaskToWorker = 0;
        try {
            while ((line = reader.readLine()) != null) {
				if(line != ""){
                    if(numOfTaskToWorker == numOfTaskPerWorker) {
                        addWorker();
                        numOfTaskToWorker = 0;
                    }
                    sqs.sendMessage(new SendMessageRequest(myQueueUrlManToWorker, "new image task " + line));
                    numOfTaskToWorker ++;
                    numberOfURLS++;
                }
            }
            System.out.println();
        }
        catch (Exception e){}
    }

    private static void addWorker() {
        try{
            IamInstanceProfileSpecification instanceP = new IamInstanceProfileSpecification();
            instanceP.setArn("arn:aws:iam::683725846471:instance-profile/WorkerRole");
            RunInstancesRequest request = new RunInstancesRequest("ami-76f0061f", 1, 1);
            request.setInstanceType(InstanceType.T2Nano.toString()); //hard ware
            request.setUserData(getWorkerUserData());
            request.withSecurityGroups("kerenshira");
            request.withKeyName("theQueen");
            request.setIamInstanceProfile(instanceP);
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            workers.addAll(instances);
        } catch (
        AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    private static String getWorkerUserData() {
        StringBuilder managerBuild = new StringBuilder();
        managerBuild.append("#!/bin/bash\n"); //start the bash
        managerBuild.append("sudo su\n");
        managerBuild.append("yum -y install java-1.8.0 \n");
        managerBuild.append("alternatives --remove java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n");
        managerBuild.append("aws s3 cp s3://" + bucketName + "/worker.zip  worker.zip\n");
        managerBuild.append("unzip worker.zip\n");
        managerBuild.append("java -jar worker.jar\n");

        return new String(Base64.encodeBase64(managerBuild.toString().getBytes()));
    }

    private static void createQueueWorkerToMan() {
        CreateQueueRequest createQueueRequestWorkerToMan = new CreateQueueRequest("ManToApp");
        myQueueUrlWorkerToMan =  sqs.createQueue(createQueueRequestWorkerToMan).getQueueUrl();
    }

    @Override
    public void run() {
        createQueueManToWorker();
        createQueueWorkerToMan();
        downloadsImage();
        readMessages();
        createSummary();
        UploadFileToS3();

    }
}
