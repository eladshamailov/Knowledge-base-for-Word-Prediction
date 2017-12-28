import java.util.Date;
import java.util.List;
import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * main function
 * 	argumantens :
 * 		args[1] = 
 * 		args[2] = 
 * 	    args[3] = 
 * 	    args[4] = 
 */

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data
//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data
public class Main {
	
    static ProfilesConfigFile profile_file = new ProfilesConfigFile("credentials");
    static AWSCredentials credentials = new ProfileCredentialsProvider(profile_file,"default").getCredentials();
    static AmazonElasticMapReduce mapReduce =new AmazonElasticMapReduceClient(credentials);
//    private static final String NEXT_ID = "-2";
    public static void main(String[] args)  {
   
    	HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
    	    .withJar("s3n://assignment2dspmor/step1.jar") // This should be a full map reduce application.
    	    .withMainClass("step1")
    	    .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data", "s3n://assignment2dspmor/");
    	 
    	StepConfig stepConfig = new StepConfig()
    	    .withName("step1")
    	    .withHadoopJarStep(hadoopJarStep)
    	    .withActionOnFailure("TERMINATE_JOB_FLOW");
    	 
    	JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
    	    .withInstanceCount(2)
    	    .withMasterInstanceType(InstanceType.M1Small.toString())
    	    .withSlaveInstanceType(InstanceType.M1Small.toString())
    	    .withHadoopVersion("2.6.0").withEc2KeyName("morKP")
    	    .withKeepJobFlowAliveWhenNoSteps(false)
    	    .withPlacement(new PlacementType("us-east-1a"));
    	
    	 
//    	RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
//    	    .withName("job1")
//    	    .withInstances(instances)
//    	    .withSteps(stepConfig)
//    	    .withLogUri("s3n://assignment2dspmor/logs/")
//    	    .withJobFlowRole("EMR_DefaultRole")
//    	    .withServiceRole("");
    	   
    	   RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                   //.withAmiVersion("3.11.0")
                   .withName("job1")
                   .withInstances(instances)
                   .withSteps(stepConfig)
                   .withJobFlowRole("EMR_EC2_DefaultRole")
                   .withServiceRole("EMR_DefaultRole")
                   .withReleaseLabel("emr-4.6.0")
                   .withLogUri("s3n://inoutlo2/logs/");/**/

    	RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
    	String jobFlowId = runJobFlowResult.getJobFlowId();
    	System.out.println("Ran job flow with id: " + jobFlowId);
    }
    
//   static String waitForCompletion(String jobFlowId) throws InterruptedException {
//        String status = "";
//        List<String> finalStatuses = Arrays.asList("TERMINATED", "TERMINATED_WITH_ERRORS");
//        List<String> allStatuses = Arrays.asList("STARTING","BOOTSTRAPPING","RUNNING","WAITING","TERMINATING","TERMINATED","TERMINATED_WITH_ERRORS");
//        ListClustersRequest request = new ListClustersRequest().withClusterStates(allStatuses);
//        while (!finalStatuses.contains(status)) {
//            Thread.sleep(5000);
//            System.out.print(".");
//            ListClustersResult result = mapReduce.listClusters(request);
//            for (ClusterSummary cluster : result.getClusters())
//                if (cluster.getId().equals(jobFlowId)) {
//                    String curr_status = cluster.getStatus().getState().toUpperCase();
//                    if (!curr_status.equals(status))
//                        System.out.println("\nJob Flow status changed: "+curr_status);
//                    status = curr_status;
//                }
//        }
//        return status;
    }
