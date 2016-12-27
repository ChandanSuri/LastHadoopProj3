package org.eduonix;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by ubu on 5/4/14.
 */
public class AWSCloudCluster {

    private static final Boolean termination_Protection = true;
    private static final int NUM_BOX = 2;

    static AmazonElasticMapReduceClient client;


    public static void main(String[] args) throws Exception {

        AWSCloudCluster cluster = new AWSCloudCluster();
        cluster.init();
        cluster.runCluster(args);
    }

    /**
     * !!!!! WARNING WARNING important !!!!!!!!!!!!!
     * see video, the code breaks here
     * you need to rename the properties file
     * AwsCredentials.properties to
     * AwsCredentials.properties
     * and paste your actual values into the
     * <access key here> <secretKey here>
     * place holders
     * <p>
     * !!!!!!! WARNING WARNING WARNING WARNING !!!!!!!!!!!!!!!!!!!
     * never store your security credentials unencrypted
     * to GITHUB or any other code repository
     */
    void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                AWSCloudCluster.class.getClassLoader().getResourceAsStream("AwsCredentials.properties"));
        client = new AmazonElasticMapReduceClient(credentials);
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));
    }


    public void runCluster(String[] st) throws Exception {
        String[] args = new String[3];

        try {
            // Configure the job flow object that will hold the steps
            HadoopJarStepConfig customConfig = new HadoopJarStepConfig();
            customConfig.withJar("s3://emrlearning/job/uber.jar");
            customConfig.withMainClass("org.eduonix.AWSCloudProcessor2");
            // temp code

            //--service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole

            args[0] = "org.eduonix.AWSCloudProcessor2";
            args[1] = "s3n://emrlearning/input";
            args[2] = "s3n://emrlearning/ouput/out2";
            customConfig.withArgs(args);
            //Custom application jar
            StepConfig stepConfig = new StepConfig()
                    .withHadoopJarStep(customConfig)
                    .withName("clusterMapReduceStep")
                    .withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER);


            StepConfig debugging = new StepConfig()
                    .withName("Enable debugging")
                    .withActionOnFailure("TERMINATE_JOB_FLOW")
                    .withHadoopJarStep(new StepFactory().newEnableDebuggingStep());

            RunJobFlowRequest request = new RunJobFlowRequest("clusterMapReduce", setNodeBoxes());
            request.withReleaseLabel("emr-4.0.0");
            request.withSteps(debugging, stepConfig);
            request.withLogUri("s3://emrlearning/");
            request.setServiceRole("EMR_DefaultRole");
            request.setJobFlowRole("EMR_EC2_DefaultRole");

            // Run job flow (and start the cluster):
            RunJobFlowResult runJobFlowResult = client.runJobFlow(request);

/**
            String jobFlowId = runJobFlowResult.getJobFlowId();

            TerminateJobFlowsRequest terminate;
            terminate = new TerminateJobFlowsRequest()
                    .withJobFlowIds(Arrays.asList(new String[]{jobFlowId}));

            client.terminateJobFlows(terminate);   */

        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    // Configure instances:
    public static JobFlowInstancesConfig setNodeBoxes() throws Exception {

        // set up instances
        JobFlowInstancesConfig box = new JobFlowInstancesConfig();
        box.setEc2KeyName("eu_ireland");
        box.setHadoopVersion("2.6.0");
        box.setInstanceCount(NUM_BOX);
        box.setTerminationProtected(termination_Protection);
        box.setMasterInstanceType(InstanceType.M1Medium.toString());
        box.setSlaveInstanceType(InstanceType.M1Medium.toString());
        return box;
    }


}
