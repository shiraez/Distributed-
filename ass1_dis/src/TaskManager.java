import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import org.apache.commons.codec.binary.Base64;
import org.apache.xmlbeans.impl.xb.xsdschema.Public;

public class TaskManager implements Runnable{
    static AmazonEC2 ec2;
    static AmazonS3 s3;
    static AmazonSQS sqs;
    static String myQueueUrlManToApp;
    static String myQueueUrlAppToMan;
    static String bucketName;
    static String key = "";
    static int numOfTaskPerWorker;
    static public List<Instance> workers = new ArrayList<>();
    private static String myQueueUrlManToWorker;
    private static String myQueueUrlWorkerToMan;
    static int numberOfURLS;
    private static String addToQueueName = "";
    static String fileName;
    public boolean endTask = false;

    public TaskManager(AmazonEC2 ec2, AmazonS3 s3, AmazonSQS sqs, String bucketName, String key,
                       int numOfTaskPerWorker, String myQueueUrlManToWorker, String myQueueUrlWorkerToMan,
                       String myQueueUrlManToApp) {
        this.ec2 = ec2;
        this.s3 = s3;
        this.sqs = sqs;
        this.bucketName = bucketName;
        this.key = key;
        this.numOfTaskPerWorker = numOfTaskPerWorker;
        this.myQueueUrlWorkerToMan = myQueueUrlWorkerToMan;
        this.myQueueUrlManToApp = myQueueUrlManToApp;
        this.myQueueUrlManToWorker = myQueueUrlManToWorker;

    }





    private static void UploadFileToS3() {
        System.out.println("Uploading a new object to S3 from a file\n");
        File file = new File("answer" + key);
        key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
        PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
        s3.putObject(req);



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



    @Override
    public void run() {
        downloadsImage();
        getRespons();
        UploadFileToS3();
        sentMassageToLocal();

    }

    private void sentMassageToLocal() {

        sqs.sendMessage(new SendMessageRequest(myQueueUrlManToApp, "done task " + "answer"+key));
    }

    private void getRespons() {
        int numOfAns = 0;
        String pattern = "done image task (.*) (.*)";
        Pattern r = Pattern.compile(pattern);
        try {
            PrintWriter writer = new PrintWriter("answer" + key, "UTF-8");
            while(numberOfURLS > numOfAns) {
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrlManToApp);
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                myWait();
                for (Message message : messages) {
                    if (message.getBody().startsWith("done image task")) {
                        Matcher m = r.matcher(message.getBody());
                        m.matches();
                        String url = m.group(1);
                        String text = m.group(2);
                        writer.write(url);
                        writer.write("------\n");
                        numOfAns++;
                        String messageRecieptHandle = message.getReceiptHandle();
                        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrlManToApp, messageRecieptHandle));

                    }
                }
            }
        }
        catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        
    }

    private void myWait() {
    }
}
