---
title: "Batch data ingestion into Amazon OpenSearch Service using AWS Glue"
url: "https://aws.amazon.com/blogs/big-data/batch-data-ingestion-into-amazon-opensearch-service-using-aws-glue/"
date: "Mon, 13 Jan 2025 20:50:32 +0000"
author: "Ravikiran Rao"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>Organizations constantly work to process and analyze vast volumes of data to derive actionable insights. Effective data ingestion and search capabilities have become essential for use cases like log analytics, application search, and enterprise search. These use cases demand a robust pipeline that can handle high data volumes and enable efficient data exploration.</p> 
<p><a href="https://spark.apache.org/docs/latest/" rel="noopener" target="_blank">Apache Spark</a>, an open source powerhouse for large-scale data processing, is widely recognized for its speed, scalability, and ease of use. Its ability to process and transform massive datasets has made it an indispensable tool in modern data engineering. <a href="https://aws.amazon.com/opensearch-service/features/" rel="noopener" target="_blank">Amazon OpenSearch Service</a>—a community-driven search and analytics solution—empowers organizations to search, aggregate, visualize, and analyze data seamlessly. Together, Spark and OpenSearch Service offer a compelling solution for building powerful data pipelines. However, ingesting data from Spark into OpenSearch Service can present challenges, especially with diverse data sources.</p> 
<p>This post showcases how to use Spark on <a href="https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html" rel="noopener" target="_blank">AWS Glue</a> to seamlessly ingest data into OpenSearch Service. We cover batch ingestion methods, share practical examples, and discuss best practices to help you build optimized and scalable data pipelines on AWS.</p> 
<h2>Overview of solution</h2> 
<p>AWS Glue is a serverless data integration service that simplifies data preparation and integration tasks for analytics, machine learning, and application development. In this post, we focus on batch data ingestion into OpenSearch Service using Spark on AWS Glue.</p> 
<p>AWS Glue offers multiple integration options with OpenSearch Service using various open source and AWS managed libraries, including:</p> 
<ul> 
 <li><a href="https://github.com/opensearch-project/opensearch-spark" rel="noopener" target="_blank">OpenSearch Spark Library</a></li> 
 <li><a href="https://github.com/elastic/elasticsearch-hadoop" rel="noopener" target="_blank">Elasticsearch Hadoop Library</a></li> 
 <li><a href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-opensearch-home.html" rel="noopener" target="_blank">AWS Glue OpenSearch Service connections</a></li> 
</ul> 
<p>In the following sections, we explore each integration method in detail, guiding you through the setup and implementation. As we progress, we incrementally build the architecture diagram shown in the following figure, providing a clear path for creating robust data pipelines on AWS. Each implementation is independent of the others. We chose to showcase them separately, because in a real-world scenario, only one of the three integration methods is likely to be used.</p> 
<p><img alt="Image showing the high level architecture diagram" class="size-full wp-image-74177 aligncenter" height="581" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-001-1.png" style="margin: 10px 0px 10px 0px;" width="941" /></p> 
<p>You can find the code base in the accompanying <a href="https://github.com/aws-samples/opensearch-glue-integration-patterns" rel="noopener" target="_blank">GitHub repo</a>. In the following sections, we walk through the steps to implement the solution.</p> 
<h2>Prerequisites</h2> 
<p>Before you deploy this solution, make sure the following prerequisites are in place:</p> 
<ul> 
 <li>Access to a valid <a href="https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fportal.aws.amazon.com%2Fbilling%2Fsignup%2Fresume&amp;client_id=signup" rel="noopener" target="_blank">AWS account</a></li> 
 <li>The latest <a href="http://aws.amazon.com/cli" rel="noopener" target="_blank">AWS Command Line Interface</a> (AWS CLI) <a href="https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" rel="noopener" target="_blank">installed</a> on your local machine</li> 
 <li><a href="https://github.com/git-guides/install-git" rel="noopener" target="_blank">git</a>, <a href="https://www.gnu.org/software/gawk/manual/gawk.html" rel="noopener" target="_blank">awk</a>, <a href="https://curl.se/" rel="noopener" target="_blank">curl</a>, and <a href="https://www.gnu.org/software/bash/" rel="noopener" target="_blank">bash</a> installed on your local machine</li> 
 <li>Permission to create AWS resources</li> 
 <li>Familiarity with Apache Spark, AWS Glue, and Amazon OpenSearch Service</li> 
</ul> 
<h2>Clone the repository to your local machine</h2> 
<p>Clone the repository to your local machine and set the <code>BLOG_DIR</code> environment variable. All the relative paths assume <code>BLOG_DIR</code> is set to the repository location in your machine. If <code>BLOG_DIR</code> is not being used, adjust the path accordingly.</p> 
<div class="hide-language"> 
 <pre><code class="lang-bash">git clone git@github.com:aws-samples/opensearch-glue-integration-patterns.git
cd opensearch-glue-integration-patterns
export BLOG_DIR=$(pwd)</code></pre> 
</div> 
<h2>Deploy the AWS CloudFormation template to create the necessary infrastructure</h2> 
<p>The main focus of this post is to demonstrate how to use the mentioned libraries in Spark on AWS Glue to ingest data into OpenSearch Service. Though we center on this core topic, several key AWS components will need to be pre-provisioned for the integration examples, such as a <a href="https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html" rel="noopener" target="_blank">Amazon Virtual Private Cloud</a> (Amazon VPC), multiple <a href="https://docs.aws.amazon.com/vpc/latest/userguide/configure-subnets.html" rel="noopener" target="_blank">Subnets</a>, an <a href="https://docs.aws.amazon.com/kms/latest/developerguide/overview.html" rel="noopener" target="_blank">AWS Key Management Service</a> (AWS KMS) key, an <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html" rel="noopener" target="_blank">Amazon Simple Storage Service</a> (Amazon S3) bucket, an AWS Glue role, and an OpenSearch Service cluster with domains for OpenSearch Service and Elasticsearch. To simplify the setup, we’ve automated the provisioning of this core infrastructure using the <code>cloudformation/opensearch-glue-infrastructure.yaml</code> <a href="https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html" rel="noopener" target="_blank">AWS CloudFormation</a> template.</p> 
<ol> 
 <li>Run the following commands</li> 
</ol> 
<p>The CloudFormation template will deploy the necessary networking components (such as VPC and subnets), <a href="https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html" rel="noopener" target="_blank">Amazon CloudWatch</a> logging, AWS Glue role, and OpenSearch Service and Elasticsearch domains required to implement the proposed architecture. Use a strong password (8–128 characters, three of which are lowercase, uppercase, numbers, or special characters, and no /, “, or spaces) and adhere to your organization’s security standards for <code>ESMasterUserPassword</code> and <code>OSMasterUserPassword</code> in the following command:</p> 
<div class="hide-language"> 
 <pre><code class="lang-bash">cd ${BLOG_DIR}/cloudformation/
aws cloudformation deploy \
--template-file ${BLOG_DIR}/cloudformation/opensearch-glue-infrastructure.yaml \
--stack-name GlueOpenSearchStack \
--capabilities CAPABILITY_NAMED_IAM \
--region <span style="color: #ff0000;">&lt;AWS_REGION&gt;</span> \
--parameter-overrides \
ESMasterUserPassword=<span style="color: #ff0000;">&lt;ES_MASTER_USER_PASSWORD&gt;</span> \
OSMasterUserPassword=<span style="color: #ff0000;">&lt;OS_MASTER_USER_PASSWORD&gt;</span></code></pre> 
</div> 
<p>You should see a success message such as <code>"Successfully created/updated stack – GlueOpenSearchStack"</code> after the resources have been provisioned successfully. Provisioning this CloudFormation stack typically takes approximately 30 minutes to complete.</p> 
<ol start="2"> 
 <li>On the AWS CloudFormation console, locate the <code>GlueOpenSearchStack</code> stack, and confirm that its status is <strong>CREATE_COMPLETE</strong>.</li> 
</ol> 
<p><img alt="Image showing the &quot;CREATE_COMPLETE&quot; status of cloudformation template" class="aligncenter size-full wp-image-74390" height="346" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/18/BDB-4806-IMAGE-002-2.png" width="1288" /></p> 
<p>You can review the deployed resources on the <strong>Resources</strong> tab, as shown in the following screenshot.The screenshot does not display all the created resources.</p> 
<p><img alt="Image showing the &quot;Resources&quot; tab of cloudformation template" class="aligncenter size-full wp-image-74184" height="1085" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-003-1.png" style="margin: 10px 0px 10px 0px;" width="1663" /></p> 
<h2>Additional setup steps</h2> 
<p>In this section, we collect essential information, including the S3 bucket name and the OpenSearch Service and Elasticsearch domain endpoints. These details are required for executing the code in subsequent sections.</p> 
<h3>Capture the details of the provisioned resources</h3> 
<p>Use the following AWS CLI command to extract and save the output values from the CloudFormation stack to a file named <code>GlueOpenSearchStack_outputs.txt</code>. We refer to the values in this file in upcoming steps.</p> 
<div class="hide-language"> 
 <pre><code class="lang-bash">aws cloudformation describe-stacks \
--stack-name GlueOpenSearchStack \
--query 'sort_by(Stacks[0].Outputs[], &amp;OutputKey)[].{Key:OutputKey,Value:OutputValue}' \
--output table \
--no-cli-pager \
--region <span style="color: #ff0000;">&lt;AWS_REGION&gt;</span> &gt; ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<h3>Download NY Green Taxi December 2022 dataset and copy to S3 bucket</h3> 
<p>The purpose of this post is to demonstrate the technical implementation of ingesting data into OpenSearch Service using AWS Glue. Understanding the dataset itself is not essential, aside from its data format, which we discuss in AWS Glue notebooks in later sections. To learn more about the dataset, you can find additional information on the <a href="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" rel="noopener" target="_blank">NYC Taxi and Limousine Commission website</a>.</p> 
<p>We specifically request that you download the December 2022 dataset, because we have tested the solution using this particular dataset:</p> 
<div class="hide-language"> 
 <pre><code class="lang-bash">S3_BUCKET_NAME=$(awk -F '|' '$2 ~ /S3Bucket/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt)
mkdir -p ${BLOG_DIR}/datasets &amp;&amp; cd ${BLOG_DIR}/datasets
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet
aws s3 cp green_tripdata_2022-12.parquet s3://${S3_BUCKET_NAME}/datasets/green_tripdata_2022-12.parquet</code></pre> 
</div> 
<h3>Download the required JARs from the Maven repository and copy to S3 bucket</h3> 
<p>We’ve specified a particular JAR file version to ensure stable deployment experience. However, we recommend adhering to your organization’s security best practices and reviewing any known vulnerabilities in the version of the JAR files before deployment. AWS does not guarantee the security of any open-source code used here. Additionally, please verify the downloaded JAR file’s checksum against the published value to confirm its integrity and authenticity.</p> 
<div class="hide-language"> 
 <pre><code class="lang-bash">mkdir -p ${BLOG_DIR}/jars &amp;&amp; cd ${BLOG_DIR}/jars
# OpenSearch Service jar
curl -O https://repo1.maven.org/maven2/org/opensearch/client/opensearch-spark-30_2.12/1.0.1/opensearch-spark-30_2.12-1.0.1.jar
aws s3 cp opensearch-spark-30_2.12-1.0.1.jar s3://${S3_BUCKET_NAME}/jars/opensearch-spark-30_2.12-1.0.1.jar
# Elasticsearch jar
curl -O https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/7.17.23/elasticsearch-spark-30_2.12-7.17.23.jar
aws s3 cp elasticsearch-spark-30_2.12-7.17.23.jar s3://${S3_BUCKET_NAME}/jars/elasticsearch-spark-30_2.12-7.17.23.jar</code></pre> 
</div> 
<p>In the following sections, we implement the individual data ingestion methods as outlined in the architecture diagram.</p> 
<h2>Ingest data into OpenSearch Service using the OpenSearch Spark library</h2> 
<p>In this section, we load an OpenSearch Service index using Spark and the <a href="http://github.com/opensearch-project/opensearch-hadoop" rel="noopener" target="_blank">OpenSearch Spark library</a>. We demonstrate this implementation by using AWS Glue notebooks, employing <a href="https://opensearch.org/docs/latest/security/authentication-backends/basic-authc/" rel="noopener" target="_blank">basic authentication using user name and password</a>.</p> 
<p>To demonstrate the ingestion mechanisms, we have provided the <code>Spark-and-OpenSearch-Code-Steps.ipynb</code> notebook with detailed instructions. Follow the steps in this section in conjunction with the instructions in the notebook.</p> 
<h3>Set up the AWS Glue Studio notebook</h3> 
<p>Complete the following steps:</p> 
<ol> 
 <li>On the AWS Glue console, choose <strong>ETL jobs</strong> in the navigation pane.</li> 
 <li>Under <strong>Create job</strong>, choose <strong>Notebook</strong>.</li> 
</ol> 
<p><img alt="Image showing AWS console page for AWS Glue to open notebook" class="aligncenter size-full wp-image-74234" height="327" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-007.png" style="margin: 10px 0px 10px 0px;" width="1797" /></p> 
<ol start="3"> 
 <li>Upload the notebook file located at <code>${BLOG_DIR}/glue_jobs/Spark-and-OpenSearch-Code-Steps.ipynb</code>.</li> 
 <li>For <strong>IAM role</strong>, choose the AWS Glue job IAM role that begins with <code>GlueOpenSearchStack-GlueRole-*</code>.</li> 
</ol> 
<p><img alt="Image showing AWS console page for AWS Glue to open notebook" class="aligncenter size-full wp-image-74262" height="527" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-008-1.png" style="margin: 10px 0px 10px 0px;" width="883" /></p> 
<ol start="5"> 
 <li>Enter a name for the notebook (for example, <code>Spark-and-OpenSearch-Code-Steps</code>) and choose <strong>Save</strong>.</li> 
</ol> 
<p><img alt="Image showing AWS Glue OpenSearch Notebook" class="aligncenter size-full wp-image-74236" height="864" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-009.png" style="margin: 10px 0px 10px 0px;" width="1711" /></p> 
<h3>Replace the placeholder values in the notebook</h3> 
<p>Complete the following steps to update the placeholders in the notebook:</p> 
<ol> 
 <li>In Step 1 in the notebook, replace the placeholder <span style="color: #ff0000;">&lt;GLUE-INTERACTIVE-SESSION-CONNECTION-NAME&gt;</span> with the AWS Glue interactive session connection name. You can get the name of the interactive session by executing the following command:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">cd ${BLOG_DIR}
awk -F '|' '$2 ~ /GlueInteractiveSessionConnectionName/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<ol start="2"> 
 <li>In Step 1 in the notebook, replace the placeholder <span style="color: #ff0000;">&lt;S3-BUCKET-NAME&gt;</span> and populate the variable <code>s3_bucket</code> with the bucket name. You can get the name of the S3 bucket by executing the following command:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">awk -F '|' '$2 ~ /S3Bucket/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<ol start="3"> 
 <li>In Step 4 in the notebook, replace <span style="color: #ff0000;">&lt;OPEN-SEARCH-DOMAIN-WITHOUT-HTTPS&gt;</span> with the OpenSearch Service domain name. You can get the domain name by executing the following command:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">awk -F '|' '$2 ~ /OpenSearchDomainEndpoint/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<h3>Run the notebook</h3> 
<p>Run each cell of the notebook to load data into the OpenSearch Service domain and read it back to verify the successful load. Refer to the detailed instructions within the notebook for execution-specific guidance.</p> 
<h3>Spark write modes (append vs. overwrite)</h3> 
<p>It is recommended to write data incrementally into OpenSearch Service indexes using the <code>append</code> mode, as demonstrated in Step 8 in the notebook. However, in certain cases, you may need to refresh the entire dataset in the OpenSearch Service index. In these scenarios, you can use the <code>overwrite</code> mode, though it is not advised for large indexes. When using <code>overwrite</code> mode, the Spark library deletes rows from the OpenSearch Service index one by one and then rewrites the data, which can be inefficient for large datasets. To avoid this, you can implement a preprocessing step in Spark to identify insertions and updates, and then write the data into OpenSearch Service using <code>append</code> mode.</p> 
<h2>Ingest data into Elasticsearch using the Elasticsearch Hadoop library</h2> 
<p>In this section, we load an Elasticsearch index using Spark and the <a href="https://github.com/elastic/elasticsearch-hadoop" rel="noopener" target="_blank">Elasticsearch Hadoop Library</a>. We demonstrate this implementation by using AWS Glue as the engine for Spark.</p> 
<h3>Set up the AWS Glue Studio notebook</h3> 
<p>Complete the following steps to set up the notebook:</p> 
<ol> 
 <li>On the AWS Glue console, choose <strong>ETL jobs</strong> in the navigation pane.</li> 
 <li>Under <strong>Create job</strong>, choose <strong>Notebook</strong>.</li> 
</ol> 
<p><img alt="Image showing AWS console page for AWS Glue to open notebook" class="aligncenter size-full wp-image-74237" height="265" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-010.png" style="margin: 10px 0px 10px 0px;" width="1430" /></p> 
<ol start="3"> 
 <li>Upload the notebook file located at <code>${BLOG_DIR}/glue_jobs/Spark-and-Elasticsearch-Code-Steps.ipynb</code>.</li> 
 <li>For <strong>IAM role</strong>, choose the AWS Glue job IAM role that begins with <code>GlueOpenSearchStack-GlueRole-*</code>.</li> 
</ol> 
<p><img alt="Image showing AWS console page for AWS Glue to open notebook" class="aligncenter size-full wp-image-74238" height="523" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-011.png" style="margin: 10px 0px 10px 0px;" width="885" /></p> 
<ol start="5"> 
 <li>Enter a name for the notebook (for example, <code>Spark-and-ElasticSearch-Code-Steps</code>) and choose <strong>Save</strong>.</li> 
</ol> 
<p><img alt="Image showing AWS Glue Elasticsearch Notebook" class="aligncenter size-full wp-image-74239" height="711" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-012-1.png" style="margin: 10px 0px 10px 0px;" width="2059" /></p> 
<h3>Replace the placeholder values in the notebook</h3> 
<p>Complete the following steps:</p> 
<ol> 
 <li>In Step 1 in the notebook, replace the placeholder <span style="color: #ff0000;">&lt;GLUE-INTERACTIVE-SESSION-CONNECTION-NAME&gt;</span> with the AWS Glue interactive session connection name. You can get the name of the interactive session by executing the following command:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">awk -F '|' '$2 ~ /GlueInteractiveSessionConnectionName/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<ol start="2"> 
 <li>In Step 1 in the notebook, replace the placeholder <span style="color: #ff0000;">&lt;S3-BUCKET-NAME&gt;</span> and populate the variable <code>s3_bucket</code> with the bucket name. You can get the name of the S3 bucket by executing the following command:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">awk -F '|' '$2 ~ /S3Bucket/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<ol start="3"> 
 <li>In Step 4 in the notebook, replace <span style="color: #ff0000;">&lt;ELASTIC-SEARCH-DOMAIN-WITHOUT-HTTPS&gt;</span> with the Elasticsearch domain name. You can get the domain name by executing the following command:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">awk -F '|' '$2 ~ /ElasticsearchDomainEndpoint/ {gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' ${BLOG_DIR}/GlueOpenSearchStack_outputs.txt</code></pre> 
</div> 
<h3>Run the notebook</h3> 
<p>Run each cell in the notebook to load data to the Elasticsearch domain and read it back to verify the successful load. Refer to the detailed instructions within the notebook for execution-specific guidance.</p> 
<h2>Ingest data into OpenSearch Service using the AWS Glue OpenSearch Service connection</h2> 
<p>In this section, we load an OpenSearch Service index using Spark and the <a href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-opensearch-home.html" rel="noopener" target="_blank">AWS Glue OpenSearch Service connection</a>.</p> 
<h3>Create the AWS Glue job</h3> 
<p>Complete the following steps to create an AWS Glue Visual ETL job:</p> 
<ol> 
 <li>On the AWS Glue console, choose <strong>ETL jobs</strong> in the navigation pane.</li> 
 <li>Under <strong>Create job</strong>, choose <strong>Visual ETL</strong></li> 
</ol> 
<p>This will open the AWS Glue job visual editor.<img alt="Image showing AWS console page for AWS Glue to open Visual ETL" class="aligncenter wp-image-74779 size-full" height="327" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/01/10/BDB-4806-IMAGE-018-1.png" width="1797" /></p> 
<ol start="3"> 
 <li>Choose the plus sign, and under <strong>Sources</strong>, choose <strong>Amazon S3</strong>.</li> 
</ol> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74249" height="513" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-019-1.png" style="margin: 10px 0px 10px 0px;" width="1668" /></p> 
<ol start="4"> 
 <li>In the visual editor, choose the <strong>Data Source – S3 bucket </strong>node.</li> 
 <li>In the <strong>Data source properties – S3 </strong>pane, configure the data source as follows:</li> 
</ol> 
<ul> 
 <li> 
  <ul> 
   <li>For <strong>S3 source type</strong>, select <strong>S3 location</strong>.</li> 
   <li>For <strong>S3 URL</strong>, choose <strong>Browse S3</strong>, and choose the <code>green_tripdata_2022-12.parquet</code> file from the designated S3 bucket.</li> 
   <li>For <strong>Data format</strong>, choose <strong>Parquet</strong>.</li> 
  </ul> </li> 
</ul> 
<ol start="6"> 
 <li>Choose <strong>Infer schema</strong> to let AWS Glue detect the schema of the data.</li> 
</ol> 
<p>This will set up your data source from the specified S3 bucket.</p> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74250" height="726" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-020.png" style="margin: 10px 0px 10px 0px;" width="1674" /></p> 
<ol start="7"> 
 <li>Choose the plus sign again to add a new node.</li> 
 <li>For <strong>Transforms</strong>, choose <strong>Drop Fields </strong>to include this transformation step.</li> 
</ol> 
<p>This will allow you to remove any unnecessary fields from your dataset before loading it into OpenSearch Service.</p> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74267" height="504" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-021.png" style="margin: 10px 0px 10px 0px;" width="1662" /></p> 
<ol start="9"> 
 <li>Choose the <strong>Drop Fields</strong> transform node, then select the following fields to drop from the dataset:</li> 
</ol> 
<ul> 
 <li> 
  <ul> 
   <li><code>payment_type</code></li> 
   <li><code>trip_type</code></li> 
   <li><code>congestion_surcharge</code></li> 
  </ul> </li> 
</ul> 
<p>This will remove these fields from the data before it is loaded into OpenSearch Service.</p> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74252" height="1195" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-022-1.png" style="margin: 10px 0px 10px 0px;" width="1663" /></p> 
<ol start="10"> 
 <li>Choose the plus sign again to add a new node.</li> 
 <li>For <strong>Targets</strong>, choose <strong>Amazon OpenSearch Service</strong>.</li> 
</ol> 
<p>This will configure OpenSearch Service as the destination for the data being processed.</p> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74253" height="413" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-023.png" style="margin: 10px 0px 10px 0px;" width="1665" /></p> 
<ol start="12"> 
 <li>Choose the <strong>Data target – Amazon OpenSearch Service</strong> node and configure it as follows:</li> 
</ol> 
<ul> 
 <li> 
  <ul> 
   <li>For <strong>Amazon OpenSearch Service connection</strong>, choose the connection <code>GlueOpenSearchServiceConnec-*</code> from the drop down.</li> 
   <li>For <strong>Index</strong>, enter <code>green_taxi</code>. The <code>green_taxi</code> index was created earlier in the “Ingest data into OpenSearch Service using the OpenSearch Spark library” section.</li> 
  </ul> </li> 
</ul> 
<p>This configures the OpenSearch Service to write the processed data to the specified index.</p> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74254" height="1117" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-024-1.png" style="margin: 10px 0px 10px 0px;" width="1891" /></p> 
<ol start="13"> 
 <li>On the <strong>Job details</strong> tab, update the job details as follows:</li> 
</ol> 
<ul> 
 <li> 
  <ul> 
   <li>For <strong>Name</strong>, enter a name (for example, <code>Spark-and-Glue-OpenSearch-Connection</code>).</li> 
   <li>For <strong>Description</strong>, enter an optional description (for example, <code>AWS Glue job using Glue OpenSearch Connection to load data into Amazon OpenSearch Service</code>).</li> 
   <li>For <strong>IAM role</strong>, choose the role starting with <code>GlueOpenSearchStack-GlueRole-*</code>.</li> 
   <li>For the <strong>Glue version</strong>, choose <code>Glue 4.0 – Supports spark 3.3, Scala 2, Python 3</code></li> 
   <li>Leave the rest of the fields as default.</li> 
   <li>Choose <strong>Save</strong> to save the changes.</li> 
  </ul> </li> 
</ul> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74272" height="566" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-025-1.png" style="margin: 10px 0px 10px 0px;" width="1667" /></p> 
<ol start="14"> 
 <li>To run the AWS Glue job Spark-and-Glue-OpenSearch-Connector, choose <strong>Run</strong>.</li> 
</ol> 
<p>This will initiate the job execution.</p> 
<p><img alt="Image showing AWS console page for AWS Glue Visual Editor" class="aligncenter size-full wp-image-74256" height="677" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-026.png" style="margin: 10px 0px 10px 0px;" width="1666" /></p> 
<ol start="15"> 
 <li>Choose the <strong>Runs</strong> tab and wait for the AWS Glue job to complete successfully.</li> 
</ol> 
<p>You will see the status change to <strong>Succeeded</strong> when the job is complete.</p> 
<p><img alt="Image showing AWS console page for AWS Glue job run status" class="aligncenter size-full wp-image-74257" height="336" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/17/BDB-4806-IMAGE-027.png" style="margin: 10px 0px 10px 0px;" width="1779" /></p> 
<h2>Clean up</h2> 
<p>To clean up your resources, complete the following steps:</p> 
<ol> 
 <li>Delete the CloudFormation stack:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash">aws cloudformation delete-stack \
--stack-name GlueOpenSearchStack \
--region <span style="color: #ff0000;">&lt;AWS_REGION&gt;</span></code></pre> 
</div> 
<ol start="2"> 
 <li>Delete the AWS Glue jobs:</li> 
</ol> 
<ul> 
 <li> 
  <ul> 
   <li>On the AWS Glue console, under <strong>ETL jobs</strong> in the navigation pane, choose <strong>Visual ETL</strong>.</li> 
   <li>Select the jobs you created (<code>Spark-and-Glue-OpenSearch-Connector</code>, <code>Spark-and-ElasticSearch-Code-Steps</code>, and <code>Spark-and-OpenSearch-Code-Steps</code>) and on the <strong>Actions </strong>menu, choose <strong>Delete</strong>.</li> 
  </ul> </li> 
</ul> 
<ul> 
 <li></li> 
</ul> 
<h2>Conclusion</h2> 
<p>In this post, we explored several ways to ingest data into OpenSearch Service using Spark on AWS Glue. We demonstrated the use of three key libraries: the <strong>AWS Glue OpenSearch Service connection</strong>, the <strong>OpenSearch Spark Library</strong>, and the <strong>Elasticsearch Hadoop Library</strong>. The methods outlined in this post can help you streamline your data ingestion into OpenSearch Service.</p> 
<p>If you’re interested in learning more and getting hands-on experience, we’ve created a workshop that walks you through the entire process in detail. You can explore the full setup for ingesting data into OpenSearch Service, handling both batch and real-time streams, and building dashboards. Check out the workshop <a href="https://catalog.us-east-1.prod.workshops.aws/workshops/962ec64d-16e6-44c7-b1b8-9b7cd7e0b21b/en-US" rel="noopener" target="_blank">Unified Real-Time Data Processing and Analytics Using Amazon OpenSearch and Apache Spark</a> to deepen your understanding and apply these techniques step by step.</p> 
<hr /> 
<h3>About the Authors</h3> 
<p style="clear: both;"><img alt="" class="wp-image-74499 size-full alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/18/rkrao.jpeg" width="100" /><strong>Ravikiran Rao</strong> is a Data Architect at Amazon Web Services and is passionate about solving complex data challenges for various customers. Outside of work, he is a theater enthusiast and amateur tennis player.</p> 
<p style="clear: both;"><img alt="" class="wp-image-12850 size-full alignleft" height="123" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2020/10/27/vishwa-gupta-100.jpg" width="100" /><strong>Vishwa Gupta</strong> is a Senior Data Architect with the AWS Professional Services Analytics Practice. He helps customers implement big data and analytics solutions. Outside of work, he enjoys spending time with family, traveling, and trying new food.</p> 
<p style="clear: both;"><img alt="" class="wp-image-34917 size-full alignleft" height="100" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2022/09/26/BDB-2467-suvojid-150x150-1.png" width="100" /><strong>Suvojit Dasgupta</strong> is a Principal Data Architect at Amazon Web Services. He leads a team of skilled engineers in designing and building scalable data solutions for AWS customers. He specializes in developing and implementing innovative data architectures to address complex business challenges.</p>
