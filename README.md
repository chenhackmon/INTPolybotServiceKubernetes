> [!NOTE]  
> This repository has a sister repo - PolybotInfra
> Take a look at it to understand how Polybot is deployed

# The Polybot Service: Kubernetes Project

## Background and goals

In this project, you will build a high-quality Kubernetes cluster on AWS using EC2 instances. 
Your tasks will include creating the cluster, deploying the Polybot service, setting up CI/CD pipelines, implementing monitoring, and enabling cluster autoscaling.

> [!WARNING]
> Never commit AWS credentials into your git repo nor your Docker containers!

Let's get started...

## Part I: Create a Kubernetes cluster 

If havn't done yet, follow the **Provision cluster on AWS with kubeadm** tutorial to create a two EC2 nodes cluster using `kubeadm`. 

## Part II: Deploy the Polybot service

The below diagram illustrates the higher level architecture:

![][k8s_project_arch]

#### The MongoDB service

The MongoDB should be deployed, similarly to the previous exercise, by a 3-replica cluster (1 primary, 2 secondaries).

> [!IMPORTANT]
> Don't be lazy and use Helm chart or Operator to deploy Mongo! Build your own YAML manifests solution.   

In order to initialize the replicaset, you should create [`Job` workload](https://kubernetes.io/docs/concepts/workloads/controllers/job/) that runs the initialization process if the cluster has not been initialized yet.  
To test the initialization, you can manually trigger the Job.

Soon you'll automate the initialization process as part of the CI/CD pipelines.  

Note that the Polybot and Yolo5 services should interact with the primary (the read/write replica) only, not the secondaries.

#### The `polybot` microservice

You'll find the code skeleton under `polybot/`.
Take a look at it - it's similar to the one used in the previous project, so you can leverage your code implementation from before.

Your Telegram Token is a sensitive data. It should be stored in [AWS Secret Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html). 
Create a corresponding secret in Secret Manager, under **Secret type** choose **Other type of secret**.

#### Deploy the `yolo5` microservice

You'll find the code skeleton under `yolo5/`.

The `app.py` contains code that periodically consumes jobs from an **SQS queue**:
 - **Polybot -> Yolo5 communication:** When the Polybot microservice receives a message from Telegram servers, it uploads the image to the S3 bucket. 
     Then, instead of talking directly with the Yolo5 microservice using a simple HTTP request, the bot sends a "job" to an SQS queue.
     The job message contains information regarding the image to be processed, as well as the Telegram `chat_id`.
     The Yolo5 microservice acts as a consumer, consumes the jobs from the queue, downloads the image from S3, processes the image, and writes the results to a **MongoDB table**.
 - **Yolo5 -> Polybot communication:** After writing the results to MongoDB, the Yolo5 microservice then sends a `POST` HTTP request to the Yolo5 microservice, to `/results?predictionId=<predictionId>`, while `<predictionId>` is the prediction ID of the job the yolo5 worker has just completed. 
   The `/results` endpoint in the Polybot microservice then should retrieve the results from the MongoDB and sends them to the end-user Telegram chat by utilizing the `chat_id` value.

#### The Nginx ingress controller

The Polybot microservice should be accessible by Telegram server using the same webhook mechanism used so far. 
But now you should not use Ngrok to expose the service, but your real Load Balancer address. 

Keep in mind that according to [Telegram's webhook docs](https://core.telegram.org/bots/webhooks), your LB should listen to either 443 or 8443, HTTPS **only**.
For that you'll need a TLS certificate. This can be achieved by [generating a self-signed certificate](https://core.telegram.org/bots/webhooks#a-self-signed-certificate) and import it to the ALB listener. The certificate `Common Name` (`CN`) must be your ALB domain name (E.g. `test-1369101568.eu-central-1.elb.amazonaws.com`), and you must pass the certificate file when setting the webhook in `bot.py` (i.e. `self.telegram_bot_client.set_webhook(..., certificate=open(CERTIFICATE_FILE_NAME, 'r'))`).

You can also create a dedicated subdomain in our registered domain in route53, and issue a real public certificate in ACM. 

You can also harden the security of your service by limit the LB security group to [the official CIDR of Telegram servers](https://core.telegram.org/bots/webhooks#the-short-version). 

## Part III: CI/CD pipeline 

Developers should be able to commit & push their changes to either the polybot or yolo5 services, and a new version would be deployed automatically in the cluster.   

#### Notes

- **Git repositories layout**:
  - Create (or use some existed) repo for the `polybot` and `yolo5` services source code. 
    You can have a [monorepo](https://en.wikipedia.org/wiki/Monorepo) for both the Polybot and Yolo5 app (as we've done until today), or a dedicated repo per microservice. 
  - Create a dedicated **PolybotInfra** repo for infrastructure resources (e.g. YAML manifests, configuration files, etc...).
- **CI**: Use GitHub Actions to create a **separated** Build pipeline per service (for the polybot and yolo5).
- **Continuous Deployment (CD)**:
    - Use ArgoCD to manage your continuous deployment processes.
    - **Every** YAML manifest you manage (`Deployments`, `Services`, `Ingress`, etc...) should be configured and managed by ArgoCD.

## Part IV: Cluster nodes autoscaling 

Your cluster is not autoscalable by default.
Adding a new worker node requires a manual provisioning of a new EC2 instance and executing the `kubeadm join` command to enroll the node to the cluster.

In this section, you'll deploy the [Cluster Autoscaler](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/aws/README.md) in your cluster to autoscale worker nodes. 

The Cluster Autoscaler utilizes **AWS Auto Scaling Groups** to scale in/out worker nodes by dynamically adjusting the size of the ASG based on resource demands.

> [!NOTE]
> As can be seen, only worker nodes are part of the ASG, while the control-plane is running as a single instance (though [can be potentially HA](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/high-availability/)). 

However, Cluster Autoscaler doesn't create the ASG for you. 
You must first create the ASG and define the Launch Template yourself, such that when a new worker node instance is created (or terminated) as part of the ASG, it should automatically join/leave the cluster.

#### Guidelines

1. Provision the necessary ASG and Launch Template:
   - Note that when a new worker instance is being launched, it should automatically run the `kubeadm join` command to join the cluster. Since the join token is valid for a limited time (and worker nodes can be created anytime), you have to ensure that a valid token is always available. Here are some ideas to handle this:
     - Periodically generate a join token on the control-plane node and store it in AWS Secrets Manager. Worker nodes can then fetch this token during initialization.
     - Create a Lambda function that connects to the control-plane over SSH and generates a new join token. This Lambda function is triggered as part of the worker node initialization process. 
   - Tag your ASG as detailed below, so the Cluster Autoscaler could identify it (change `<YOUR CLUSTER NAME>` accordingly):
     ```text
     k8s.io/cluster-autoscaler/enabled=true
     k8s.io/cluster-autoscaler/<YOUR CLUSTER NAME>=owned
     ```

2. Attach the following policy to your cluster roles: 

    ```json
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "autoscaling:DescribeAutoScalingGroups",
            "autoscaling:DescribeAutoScalingInstances",
            "autoscaling:DescribeLaunchConfigurations",
            "autoscaling:DescribeScalingActivities",
            "ec2:DescribeImages",
            "ec2:DescribeInstanceTypes",
            "ec2:DescribeLaunchTemplateVersions",
            "ec2:GetInstanceTypesFromInstanceRequirements",
            "eks:DescribeNodegroup"
          ],
          "Resource": ["*"]
        },
        {
          "Effect": "Allow",
          "Action": [
            "autoscaling:SetDesiredCapacity",
            "autoscaling:TerminateInstanceInAutoScalingGroup"
          ],
          "Resource": ["*"]
        }
      ]
    }
    ```
   
3. Install the Cluster Autoscaler by:

   ```bash
   helm repo add autoscaler https://kubernetes.github.io/autoscaler
   helm install cluster-autoscaler autoscaler/cluster-autoscaler --set autoDiscovery.clusterName=<YOUR-CLUSTER-NAME> --set awsRegion=<YOUR-AWS-REGION>
   ```
   
   While changing `<YOUR-CLUSTER-NAME>` and `<YOUR-AWS-REGION>` accordingly.

4. Test your cluster node auto-scalability. 

# Good Luck


[k8s_project_arch]: https://exit-zero-academy.github.io/DevOpsTheHardWayAssets/img/k8s_project_arch.png

