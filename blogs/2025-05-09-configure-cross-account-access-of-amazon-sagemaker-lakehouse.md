---
title: "Configure cross-account access of Amazon SageMaker Lakehouse multi-catalog tables using AWS Glue 5.0 Spark"
url: "https://aws.amazon.com/blogs/big-data/configure-cross-account-access-of-amazon-sagemaker-lakehouse-multi-catalog-tables-using-aws-glue-5-0-spark/"
date: "Fri, 09 May 2025 17:18:44 +0000"
author: "Aarthi Srinivasan"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>Many organizations build and operate enterprise-wide data mesh architectures using the <a href="https://aws.amazon.com/glue/" rel="noopener" target="_blank">AWS Glue</a> <a href="https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html" rel="noopener" target="_blank">Data Catalog</a> and <a href="https://aws.amazon.com/lake-formation/" rel="noopener" target="_blank">AWS Lake Formation</a> for their <a href="https://aws.amazon.com/s3/" rel="noopener" target="_blank">Amazon Simple Storage Service</a> (Amazon S3) based data lakes. Now, with <a href="https://aws.amazon.com/sagemaker/lakehouse/" rel="noopener" target="_blank">Amazon SageMaker Lakehouse</a>, these organizations can unify their data analytics and AI/ML workflows while maintaining secure cross-account access without data replication. By centralizing access to a single copy of data and using the secure fine-grained permissions of Lake Formation, enterprises can accelerate their analytics initiatives while reducing operational complexity across business units.</p> 
<p>SageMaker Lakehouse organizes data using logical containers called <em>catalogs</em>, enabling teams to seamlessly query and analyze data across their entire ecosystem—from S3 data lakes to <a href="https://aws.amazon.com/redshift/" rel="noopener" target="_blank">Amazon Redshift</a> warehouses—using familiar <a href="https://iceberg.apache.org/" rel="noopener" target="_blank">Apache Iceberg</a> compatible tools. Organizations can either mount their existing data warehouse to the lakehouse or create new catalogs using <a href="https://aws.amazon.com/redshift/features/ra3/" rel="noopener" target="_blank">Amazon Redshift managed storage</a>. Built-in zero-ETL connectors reduce data silos by integrating various data sources, enabling unified analytics across teams. This seamless integration particularly benefits existing AWS customers who already use the Data Catalog and Lake Formation, because they can immediately take advantage of SageMaker Lakehouse capabilities.</p> 
<p>AWS Glue is a serverless service that makes data integration simpler, faster, and cheaper. We <a href="https://aws.amazon.com/about-aws/whats-new/2024/12/aws-glue-5-0/" rel="noopener" target="_blank">launched AWS Glue 5.0</a> with upgraded <a href="https://spark.apache.org/releases/spark-release-3-5-4.html" rel="noopener" target="_blank">Apache Spark 3.5.4</a> and <a href="https://docs.python.org/3/whatsnew/3.11.html" rel="noopener" target="_blank">Python 3.11</a>. AWS Glue 5.0 adds support for SageMaker Lakehouse to unify your data across S3 data lakes and Redshift data warehouses.</p> 
<p>In our previous <a href="https://aws.amazon.com/blogs/big-data/simplify-data-access-for-your-enterprise-using-amazon-sagemaker-lakehouse/" rel="noopener" target="_blank">blog post</a>, we demonstrated the process of creating tables in both the Amazon Redshift managed catalog and Amazon Redshift federated catalog within a single AWS account. In this post, we show you how to share a Redshift table and Amazon S3 based Iceberg table from the account that owns the data to another account that consumes the data. In the recipient account, we run a join query on the shared data lake and data warehouse tables using Spark in AWS Glue 5.0. We walk you through the complete cross-account setup and provide the Spark configuration in a Python notebook.</p> 
<h2>Solution overview</h2> 
<p>To demonstrate the functionality of SageMaker Lakehouse multi-catalog tables using AWS Glue 5.0 Spark, let’s assume the retail company Example Retail Corp launches a campaign to understand their market and drive growth by country of operation. Their infrastructure consists of a Redshift data warehouse for structured data and an S3 data lake for structured and semi-structured data. The marketing team realizes that customer data is spread across those two systems and wants to use the support of their data engineering and analysts to analyze and provide insights. As a company, they prefer unified governance for managing data access while enabling a secure sharing mechanism for business and engineering teams.</p> 
<p>Let’s see how they can achieve the goal using SageMaker Lakehouse. The solution is represented in the following diagram.</p> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/001-BDB-5089.png"><img alt="001-BDB 5089" class="alignnone size-full wp-image-77863" height="504" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/001-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="936" /></a></p> 
<p>The setup could be extended to enterprise data meshes where a data producer account will own the Redshift clusters, catalog the tables in a central governance account, and share with any number of consumer accounts from the central account. Multiple consumer accounts could analyze the shared Redshift tables using the SageMaker Lakehouse integrated analytics engines.</p> 
<p>The solution also works for cross-Region table access. You would create a resource link for the catalog tables in an AWS Region where you want to run your analyses and create dashboards. For cross-Region resource link setup, refer to <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/setup-cross-region-access.html" rel="noopener" target="_blank">Setting up cross-Region table access</a>.</p> 
<h2>Prerequisites</h2> 
<p>To implement this solution, you need the following prerequisites:</p> 
<ul> 
 <li>Two AWS accounts with Lake Formation <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/optimize-ram.html" rel="noopener" target="_blank">cross-account sharing version 4</a> and Lake Formation administrator configured. Refer to the Lake Formation <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/permissions-reference.html#persona-dl-admin" rel="noopener" target="_blank">data administrator permissions</a> and initial <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/initial-lf-config.html" rel="noopener" target="_blank">setup of Lake Formation</a>.</li> 
 <li>Permissions from <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/redshift-ns-prereqs.html" rel="noopener" target="_blank">Prerequisites for managing Amazon Redshift namespaces in the AWS Glue Data Catalog</a> granted to the Lake Formation administrator role on both accounts.</li> 
 <li>An S3 bucket in the producer account to host the sample Iceberg table data.</li> 
 <li>An <a href="https://aws.amazon.com/iam/" rel="noopener" target="_blank">AWS Identity and Access Management</a> (IAM) role, <code>LakeFormationS3Registration_custom</code>, in the producer account to register your Iceberg table’s Amazon S3 location with Lake Formation. For details, refer to <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/register-location.html" rel="noopener" target="_blank">Registering an Amazon S3 location</a> and <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/registration-role.html" rel="noopener" target="_blank">Requirements for roles used to register locations</a>.</li> 
 <li>An <a href="https://aws.amazon.com/redshift/redshift-serverless/" rel="noopener" target="_blank">Amazon Redshift Serverless</a> namespace in the producer account. Follow the instructions in <a href="https://docs.aws.amazon.com/redshift/latest/gsg/new-user-serverless.html#serverless-console-resource-creation" rel="noopener" target="_blank">Creating a data warehouse with Amazon Redshift Serverless</a> to launch a serverless namespace with default settings.</li> 
 <li>Two sample datasets, orders and returns, in CSV format. This is Example Retail Corp’s data on their customer purchase and return trends. Their marketing team has collected these data in a Redshift table and Amazon S3 from various systems. The instructions to create these tables are provided in the appendix at the end of this post. After completing the steps in the appendix, you should have <code>customerdb.returnstbl_iceberg</code> in your default catalog and <code>ordersdb.orderstbl</code> in your Redshift Serverless application default namespace.</li> 
 <li>An IAM role, <code>Glue-execution-role</code>, in the consumer account, with the following policies: 
  <ol type="a"> 
   <li>AWS managed policies <code>AWSGlueServiceRole</code> and <code>AmazonRedshiftDataFullAccess</code>.</li> 
   <li>Create a new in-line policy with the following permissions and attach it: 
    <div class="hide-language"> 
     <pre><code class="lang-code">{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "LFandRSserverlessAccess",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "redshift-serverless:GetCredentials"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "glue.amazonaws.com"
                }
            }
        }
    ]
}</code></pre> 
    </div> </li> 
   <li>Add the following trust policy to <code>Glue-execution-role</code>, allowing AWS Glue to assume this role: 
    <div class="hide-language"> 
     <pre><code class="lang-code">{
&nbsp;&nbsp;&nbsp; "Version": "2012-10-17",
&nbsp;&nbsp;&nbsp; "Statement": [
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "Effect": "Allow",
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "Principal": {
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "Service": [
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "glue.amazonaws.com"
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ]
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; },
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "Action": "sts:AssumeRole"
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; }
&nbsp;&nbsp;&nbsp; ]
}</code></pre> 
    </div> </li> 
  </ol> <h2>Steps for producer account setup</h2> <p>For the producer account setup, you can either use your IAM administrator role added as Lake Formation administrator or use a Lake Formation administrator role with permissions added as discussed in the prerequisites. For illustration purposes, we use the IAM admin role <code>Admin</code> added as Lake Formation administrator.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/002-BDB-5089.png"><img alt="002-BDB 5089" class="alignnone size-full wp-image-77862" height="528" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/002-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="936" /></a></p> <h3>Configure your catalog</h3> <p>Complete the following steps to set up your catalog:</p> 
  <ol> 
   <li>Log in to <a href="http://aws.amazon.com/console" rel="noopener" target="_blank">AWS Management Console</a> as <code>Admin</code>.</li> 
   <li>On the Amazon Redshift console, follow the instructions in <a href="https://docs.aws.amazon.com/redshift/latest/dg/iceberg-integration-register.html" rel="noopener" target="_blank">Registering Amazon Redshift clusters and namespaces to the AWS Glue Data Catalog</a>.</li> 
   <li>After the registration is initiated, you will see the invite from Amazon Redshift on the Lake Formation console.</li> 
   <li>Select the pending catalog invitation and choose <strong>Approve and create catalog</strong>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/003-BDB-5089.png"><img alt="003-BDB 5089" class="alignnone size-full wp-image-77861" height="568" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/003-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="5"> 
   <li>On the <strong>Set catalog details</strong> page, configure your catalog: 
    <ol type="a"> 
     <li>For <strong>Name</strong>, enter a name (for this post, <code>redshiftserverless1-uswest2</code>).</li> 
     <li>Select <strong>Access this catalog from Apache Iceberg compatible engines</strong>.</li> 
     <li>Choose the IAM role you created for the data transfer.</li> 
     <li>Choose <strong>Next</strong>.</li> 
    </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/004-BDB-5089.png"><img alt="004-BDB 5089" class="alignnone size-full wp-image-77860" height="944" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/004-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p></li> 
   <li>On the <strong>Grant permissions – optional</strong> page, choose <strong>Add permissions</strong>. 
    <ol type="a"> 
     <li>Grant the <code>Admin</code> user<strong> Super user </strong>permissions for <strong>Catalog permissions</strong> and <strong>Grantable permissions</strong>.</li> 
     <li>Choose<strong> Add</strong>.</li> 
    </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/005-BDB-5089.png"><img alt="005-BDB 5089" class="alignnone size-full wp-image-77859" height="968" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/005-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p></li> 
   <li>Verify the granted permission on the next page and choose <strong>Next</strong>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/006-BDB-5089.png"><img alt="006-BDB 5089" class="alignnone size-full wp-image-77858" height="378" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/006-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></li> 
   <li>Review the details on the <strong>Review and create</strong> page and choose <strong>Create catalog</strong>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/007-BDB-5089.png"><img alt="007-BDB 5089" class="alignnone size-full wp-image-77857" height="794" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/007-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1304" /></a></li> 
  </ol> <p>Wait a few seconds for the catalog to show up.</p> 
  <ol start="9"> 
   <li>Choose <strong>Catalogs</strong> in the navigation pane and verify that the <code>redshiftserverless1-uswest2</code> catalog is created.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/008-BDB-5089.png"><img alt="008-BDB 5089" class="alignnone size-full wp-image-77856" height="595" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/008-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></li> 
   <li>Explore the catalog detail page to verify the <code>ordersdb.public</code> database.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/009-BDB-5089.png"><img alt="009-BDB 5089" class="alignnone size-full wp-image-77855" height="378" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/009-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></li> 
   <li>On the database <strong>View</strong> dropdown menu, view the table and verify that the <code>orderstbl</code> table shows up.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/010-BDB-5089.png"><img alt="010-BDB 5089" class="alignnone size-full wp-image-77854" height="336" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/010-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></li> 
  </ol> <p>As the <code>Admin</code> role, you can also query the <code>orderstbl</code> in <a href="http://aws.amazon.com/athena" rel="noopener" target="_blank">Amazon Athena</a> and confirm the data is available.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/011-BDB-5089.png"><img alt="011-BDB 5089" class="alignnone size-full wp-image-77853" height="711" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/011-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <h3>Grant permissions on the tables from the producer account to the consumer account</h3> <p>In this step, we share the Amazon Redshift federated catalog database <code>redshiftserverless1-uswest2:ordersdb.public</code> and table <code>orderstbl</code> as well as the Amazon S3 based Iceberg table <code>returnstbl_iceberg</code> and its database <code>customerdb</code> from the default catalog to the consumer account. We can’t share the entire catalog to external accounts as a catalog-level permission; we just share the database and table.</p> 
  <ol> 
   <li>On the Lake Formation console, choose <strong>Data permissions</strong> in the navigation pane.</li> 
   <li>Choose <strong>Grant</strong>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/012-BDB-5089.png"><img alt="012-BDB 5089" class="alignnone size-full wp-image-77852" height="400" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/012-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="872" /></a></li> 
   <li>Under <strong>Principals</strong>, select <strong>External accounts</strong>.</li> 
   <li>Provide the consumer account ID.</li> 
   <li>Under <strong>LF-Tags or catalog resources</strong>, select <strong>Named Data Catalog resources</strong>.</li> 
   <li>For <strong>Catalogs</strong>, choose the account ID that represents the default catalog.</li> 
   <li>For <strong>Databases</strong>, choose <code>customerdb</code>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/013-BDB-5089.png"><img alt="013-BDB 5089" class="alignnone size-full wp-image-77851" height="842" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/013-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="598" /></a></li> 
   <li>Under <strong>Database permissions</strong>, select <strong>Describe</strong> under <strong>Database permissions</strong> and <strong>Grantable permissions</strong>.</li> 
   <li>Choose <strong>Grant</strong>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/014-BDB-5089.png"><img alt="014-BDB 5089" class="alignnone size-full wp-image-77850" height="455" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/014-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="928" /></a></li> 
   <li>Repeat these steps and grant table-level <strong>Select</strong> and <strong>Describe</strong> permissions on <code>returnstbl_iceberg</code>.</li> 
   <li>Repeat these steps again to grant database- and table-level permissions for the <code>ordertbl</code> table of the federated catalog database <code>redshiftserverless1-uswest2/ordersdb</code>.</li> 
  </ol> <p>The following screenshots show the configuration for database-level permissions.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/015-BDB-5089.png"><img alt="015-BDB 5089" class="alignnone size-full wp-image-77849" height="832" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/015-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="620" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/016-BDB-5089.png"><img alt="016-BDB 5089" class="alignnone size-full wp-image-77848" height="290" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/016-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="630" /></a></p> <p>The following screenshots show the configuration for table-level permissions.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/017-BDB-5089.png"><img alt="017-BDB 5089" class="alignnone size-full wp-image-77847" height="884" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/017-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="656" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/018-BDB-5089.png"><img alt="018-BDB 5089" class="alignnone size-full wp-image-77846" height="422" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/018-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="656" /></a></p> 
  <ol start="12"> 
   <li>Choose <strong>Data permissions</strong> in the navigation pane and verify that the consumer account has been granted database- and table-level permissions for both <code>orderstbl</code> from the federated catalog and <code>returnstbl_iceberg</code> from the default catalog.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/019-BDB-5089.png"><img alt="019-BDB 5089" class="alignnone size-full wp-image-77845" height="358" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/019-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="860" /></a></li> 
  </ol> <h3>Register the Amazon S3 location of the returnstbl_iceberg with Lake Formation.</h3> <p>In this step, we register the Amazon S3 based Iceberg table <code>returnstbl_iceberg</code> data location with Lake Formation to be managed by Lake Formation permissions. Complete the following steps:</p> 
  <ol> 
   <li>On the Lake Formation console, choose <strong>Data lake locations</strong> in the navigation pane.</li> 
   <li>Choose <strong>Register location</strong>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/020-BDB-5089.png"><img alt="020-BDB 5089" class="alignnone size-full wp-image-77844" height="522" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/020-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1127" /></a></li> 
   <li>For <strong>Amazon S3 path</strong>, enter the path<strong> for your S3 bucket </strong>that you provided while creating the Iceberg table <code>returnstbl_iceberg</code>.</li> 
   <li>For <strong>IAM role</strong>, provide the user-defined role <code>LakeFormationS3Registration_custom</code> that you created as a prerequisite.</li> 
   <li>For <strong>Permission mode</strong>, select <strong>Lake Formation</strong>.</li> 
   <li>Choose <strong>Register location</strong>.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/021-BDB-5089.png"><img alt="021-BDB 5089" class="alignnone size-full wp-image-77843" height="543" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/021-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1142" /></a></li> 
   <li>Choose <strong>Data lake locations </strong>in the navigation pane to verify the Amazon S3 registration.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/022-BDB-5089.png"><img alt="022-BDB 5089" class="alignnone size-full wp-image-77842" height="172" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/022-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="780" /></a></li> 
  </ol> <p>With this step, the producer account setup is complete.</p> <h2>Steps for consumer account setup</h2> <p>For the consumer account setup, we use the IAM admin role <code>Admin</code>, added as a Lake Formation administrator.</p> <p>The steps in the consumer account are quite involved. In the consumer account, a Lake Formation administrator will accept the <a href="https://aws.amazon.com/ram/" rel="noopener" target="_blank">AWS Resource Access Manager</a> (AWS RAM) shares and create the required resource links that point to the shared catalog, database, and tables. The Lake Formation admin verifies that the shared resources are accessible by running test queries in Athena. The admin further grants permissions to the role <code>Glue-execution-role</code> on the resource links, database, and tables. The admin then runs a join query in AWS Glue 5.0 Spark using <code>Glue-execution-role</code>.</p> <h3>Accept and verify the shared resources</h3> <p>Lake Formation uses AWS RAM shares to enable cross-account sharing with Data Catalog resource policies in the AWS RAM policies. To view and verify the shared resources from producer account, complete the following steps:</p> 
  <ol> 
   <li>Log in to the consumer AWS console and set the AWS Region to match the producer’s shared resource Region. For this post, we use <code>us-west-2</code>.</li> 
   <li>Open the Lake Formation console. You will see a message indicating there is a pending invite and asking you accept it on the AWS RAM console.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/023-BDB-5089.png"><img alt="023-BDB 5089" class="alignnone size-full wp-image-77841" height="234" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/023-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="936" /></a></li> 
   <li>Follow the instructions in <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/accepting-ram-invite.html" rel="noopener" target="_blank">Accepting a resource share invitation from AWS RAM</a> to review and accept the pending invites.</li> 
   <li>When the invite status changes to <strong>Accepted</strong>, choose <strong>Shared resources</strong> under <strong>Shared with me</strong> in the navigation pane.</li> 
   <li>Verify that the Redshift Serverless federated catalog <code>redshiftserverless1-uswest2</code>, the default catalog database <code>customerdb</code>, the table <code>returnstbl_iceberg</code>, and the producer account ID under <strong>Owner ID</strong> column display correctly.<br /> <a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/024-BDB-5089.png"><img alt="024-BDB 5089" class="alignnone size-full wp-image-77840" height="326" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/024-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="936" /></a></li> 
   <li>On the Lake Formation console, under <strong>Data Catalog</strong> in the navigation pane, choose <strong>Databases</strong>.</li> 
   <li>Search by the producer account ID.<br /> You should see the <code>customerdb</code> and <code>public</code> databases. You can further select each database and choose <strong>View tables</strong> on the <strong>Actions</strong> dropdown menu and verify the table names</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/025-BDB-5089.png"><img alt="025-BDB 5089" class="alignnone size-full wp-image-77839" height="242" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/025-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="936" /></a></p> <p>You will not see an AWS RAM share invite for the catalog level on the Lake Formation console, because catalog-level sharing isn’t possible. You can review the shared federated catalog and Amazon Redshift managed catalog names on the AWS RAM console, or using the <a href="http://aws.amazon.com/cli" rel="noopener" target="_blank">AWS Command Line Interface</a> (AWS CLI) or SDK.</p> <h3>Create a catalog link container and resource links</h3> <p>A <em>catalog link container</em> is a Data Catalog object that references a local or cross-account federated database-level catalog from other AWS accounts. For more details, refer to <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/catalog-resource-link.html" rel="noopener" target="_blank">Accessing a shared federated catalog</a>. Catalog link containers are essentially Lake Formation resource links at the catalog level that reference or point to a Redshift cluster federated catalog or Amazon Redshift managed catalog object from other accounts.</p> <p>In the following steps, we create a catalog link container that points to the producer shared federated catalog <code>redshiftserverless1-uswest2</code>. Inside the catalog link container, we create a database. Inside the database, we create a resource link for the table that points to the shared federated catalog table <code>&lt;&lt;producer account id&gt;&gt;:redshiftserverless1-uswest2/ordersdb.public.orderstbl</code>.</p> 
  <ol> 
   <li>On the Lake Formation console, under <strong>Data Catalog</strong> in the navigation pane, choose <strong>Catalogs</strong>.</li> 
   <li>Choose <strong>Create catalog</strong>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/026-BDB-5089.png"><img alt="026-BDB 5089" class="alignnone size-full wp-image-77838" height="479" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/026-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="3"> 
   <li>Provide the following details for the catalog: 
    <ol type="a"> 
     <li>For <strong>Name</strong>, enter a name for the catalog (for this post, <code>rl_link_container_ordersdb</code>).</li> 
     <li>For <strong>Type</strong>, choose <strong>Catalog Link container</strong>.</li> 
     <li>For <strong>Source</strong>, choose <strong>Redshift</strong>.</li> 
     <li>For <strong>Target Redshift Catalog</strong>, enter the Amazon Resource Name (ARN) of the producer federated catalog (<code>arn:aws:glue:us-west-2:&lt;&lt;producer account id&gt;&gt;:catalog/redshiftserverless1-uswest2/ordersdb</code>).</li> 
     <li>Under <strong>Access from engines</strong>, select <strong>Access this catalog from Apache Iceberg compatible engines</strong>.</li> 
     <li>For <strong>IAM role</strong>, provide the Redshift-S3 data transfer role that you had created in the prerequisites.</li> 
     <li>Choose <strong>Next</strong>.</li> 
    </ol> </li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/027-BDB-5089.png"><img alt="027-BDB 5089" class="alignnone size-full wp-image-77837" height="592" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/027-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="4"> 
   <li>On the <strong>Grant permissions – optional</strong> page, choose <strong>Add permissions</strong>. 
    <ol type="a"> 
     <li>Grant the <code>Admin</code> user <strong>Super user</strong> permissions for <strong>Catalog permissions</strong> and <strong>Grantable permissions</strong>.</li> 
     <li>Choose <strong>Add</strong> and then choose <strong>Next</strong>.</li> 
    </ol> </li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/028-BDB-5089.png"><img alt="028-BDB 5089" class="alignnone size-full wp-image-77836" height="354" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/028-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="5"> 
   <li>Review the details on the <strong>Review and create</strong> page and choose <strong>Create catalog</strong>.</li> 
  </ol> <p>Wait a few seconds for the catalog to show up.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/029-BDB-5089.png"><img alt="029-BDB 5089" class="alignnone size-full wp-image-77835" height="598" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/029-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="6"> 
   <li>In the navigation pane, choose <strong>Catalogs</strong>.</li> 
   <li>Verify that <code>rl_link_container_ordersdb</code> is created.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/030-BDB-5089.png"><img alt="030-BDB 5089" class="alignnone size-full wp-image-77834" height="543" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/030-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <h3>Create a database under rl_link_container_ordersdb</h3> <p>Complete the following steps:</p> 
  <ol> 
   <li>On the Lake Formation console, under <strong>Data Catalog </strong>in the navigation pane, choose <strong>Databases</strong>.</li> 
   <li>On the <strong>Choose catalog</strong> dropdown menu, choose <code>rl_link_container_ordersdb</code>.</li> 
   <li>Choose <strong>Create database</strong>.</li> 
  </ol> <p>Alternatively, you can choose the <strong>Create</strong> dropdown menu and then choose <strong>Database</strong>.</p> 
  <ol start="4"> 
   <li>Provide details for the database: 
    <ol type="a"> 
     <li>For <strong>Name</strong>, enter a name (for this post, <code>public_db</code>).</li> 
     <li>For <strong>Catalog</strong>, choose <code>rl_link_container_ordersdb</code>.</li> 
     <li>Leave <strong>Location – optional</strong> as blank.</li> 
     <li>Under <strong>Default permissions for newly created tables</strong>, deselect <strong>Use only IAM access control for new tables in this database</strong>.</li> 
     <li>Choose <strong>Create database</strong>.</li> 
    </ol> </li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/031-BDB-5089.png"><img alt="031-BDB 5089" class="alignnone size-full wp-image-77833" height="628" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/031-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> 
  <ol start="5"> 
   <li>Choose <strong>Catalogs</strong> in the navigation pane to verify that <code>public_db</code> is created under <code>rl_link_container_ordersdb</code>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/032-BDB-5089.png"><img alt="032-BDB 5089" class="alignnone size-full wp-image-77832" height="427" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/032-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <h3>Create a table resource link for the shared federated catalog table</h3> <p>A resource link to a shared federated catalog table can reside only inside the database of a catalog link container. A resource link for such tables will not work if created inside the default catalog. For more details on resource links, refer to <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/create-resource-link-table.html" rel="noopener" target="_blank">Creating a resource link to a shared Data Catalog table</a>.</p> <p>Complete the following steps to create a table resource link:</p> 
  <ol> 
   <li>On the Lake Formation console, under <strong>Data Catalog</strong> in the navigation pane, choose <strong>Tables</strong>.</li> 
   <li>On the <strong>Create </strong>dropdown menu, choose <strong>Resource link</strong>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/033-BDB-5089.png"><img alt="033-BDB 5089" class="alignnone size-full wp-image-77831" height="189" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/033-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="3"> 
   <li>Provide details for the table resource link: 
    <ol type="a"> 
     <li>For<strong> Resource link name</strong>, enter a name (for this post, <code>rl_orderstbl</code>).</li> 
     <li>For <strong>Destination catalog</strong>, choose <code>rl_link_container_ordersdb</code>.</li> 
     <li>For <strong>Database</strong>, choose <code>public_db</code>.</li> 
     <li>For <strong>Shared table’s region</strong>, choose <strong>US West (Oregon)</strong>.</li> 
     <li>For <strong>Shared table</strong>, choose <code>orderstbl</code>.</li> 
     <li>After the <strong>Shared table</strong> is selected, <strong>Shared table’s database</strong> and <strong>Shared table’s catalog ID</strong> should get automatically populated.</li> 
     <li>Choose <strong>Create</strong>.</li> 
    </ol> </li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/034-BDB-5089.png"><img alt="034-BDB 5089" class="alignnone size-full wp-image-77830" height="658" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/034-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> 
  <ol start="4"> 
   <li>In the navigation pane, choose <strong>Databases</strong> to verify that <code>rl_orderstbl</code> is created under <code>public_db</code>, inside <code>rl_link_container_ordersdb</code>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/035-BDB-5089.png"><img alt="035-BDB 5089" class="alignnone size-full wp-image-77829" height="284" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/035-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/036-BDB-5089.png"><img alt="036-BDB 5089" class="alignnone size-full wp-image-77828" height="284" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/036-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <h3>Create a database resource link for the shared default catalog database.</h3> <p>Now we create a database resource link in the default catalog to query the Amazon S3 based Iceberg table shared from the producer. For details on database resource links, refer <a href="https://docs.aws.amazon.com/lake-formation/latest/dg/create-resource-link-database.html" rel="noopener" target="_blank">Creating a resource link to a shared Data Catalog database</a>.</p> <p>Though we are able to see the shared database in the default catalog of the consumer, a resource link is required to query from analytics engines, such as Athena, <a href="https://aws.amazon.com/emr/" rel="noopener" target="_blank">Amazon EMR</a>, and AWS Glue. When using AWS Glue with Lake Formation tables, the resource link needs to be named identically to the source account’s resource. For additional details on using AWS Glue with Lake Formation, refer to <a href="https://docs.aws.amazon.com/glue/latest/dg/security-lf-enable-considerations.html" rel="noopener" target="_blank">Considerations and limitations</a>.</p> <p>Complete the following steps to create a database resource link:</p> 
  <ol> 
   <li>On the Lake Formation console, under <strong>Data Catalog</strong> in the navigation pane, choose <strong>Databases</strong>.</li> 
   <li>On the <strong>Choose catalog</strong> dropdown menu, choose the account ID to choose the default catalog.</li> 
   <li>Search for <code>customerdb</code>.</li> 
  </ol> <p>You should see the shared database name <code>customerdb</code> with the <strong>Owner account ID</strong> as that of your producer account ID.</p> 
  <ol start="4"> 
   <li>Select <code>customerdb</code>, and on the <strong>Create </strong>dropdown menu, choose <strong>Resource link</strong>.</li> 
   <li>Provide details for the resource link: 
    <ol type="a"> 
     <li>For<strong> Resource link name</strong>, enter a name (for this post, <code>customerdb</code>).</li> 
     <li>The rest of the fields should be already populated.</li> 
     <li>Choose <strong>Create</strong>.</li> 
    </ol> </li> 
   <li>In the navigation pane, choose <strong>Databases</strong> and verify that <code>customerdb</code> is created under the default catalog. Resource link names will show in italicized font.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/037-BDB-5089.png"><img alt="037-BDB 5089" class="alignnone size-full wp-image-77827" height="226" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/037-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <h3>Verify access as Admin using Athena</h3> <p>Now you can verify your access using Athena. Complete the following steps:</p> 
  <ol> 
   <li>Open the Athena console.</li> 
   <li>Make sure an S3 bucket is provided to store the Athena query results. For details, refer to <a href="https://docs.aws.amazon.com/athena/latest/ug/query-results-specify-location-console.html" rel="noopener" target="_blank">Specify a query result location using the Athena console</a>.</li> 
   <li>In the navigation pane, verify both the default catalog and federated catalog tables by previewing them.</li> 
   <li>You can also run a join query as follows. Pay attention to the three-point notation for referring to the tables from two different catalogs:</li> 
  </ol> 
  <div class="hide-language"> 
   <pre><code class="lang-sql">SELECT
returns_tb.market as Market,
sum(orders_tb.quantity) as Total_Quantity
FROM rl_link_container_ordersdb.public_db.rl_orderstbl as orders_tb
JOIN awsdatacatalog.customerdb.returnstbl_iceberg as returns_tb
ON orders_tb.order_id = returns_tb.order_id
GROUP BY returns_tb.market;</code></pre> 
  </div> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/038-BDB-5089.png"><img alt="038-BDB 5089" class="alignnone size-full wp-image-77826" height="598" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/038-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <p>This verifies the new capability of SageMaker Lakehouse, which enables accessing Redshift cluster tables and Amazon S3 based Iceberg tables in the same query, across AWS accounts, through the Data Catalog, using Lake Formation permissions.</p> <h3>Grant permissions to Glue-execution-role</h3> <p>Now we will share the resources from the producer account with additional IAM principals in the consumer account. Usually, the data lake admin grants permissions to data analysts, data scientists, and data engineers in the consumer account to do their job functions, such as processing and analyzing the data.</p> <p>We set up Lake Formation permissions on the catalog link container, databases, tables, and resource links to the AWS Glue job execution role <code>Glue-execution-role</code> that we created in the prerequisites.</p> <p>Resource links allow only <strong>Describe</strong> and <strong>Drop</strong> permissions. You need to use the <strong>Grant on target</strong> configuration to provide database <strong>Describe</strong> and table <strong>Select</strong> permissions.</p> <p>Complete the following steps:</p> 
  <ol> 
   <li>On the Lake Formation console, choose <strong>Data permissions</strong> in the navigation pane.</li> 
   <li>Choose <strong>Grant</strong>.</li> 
   <li>Under <strong>Principals</strong>, select <strong>IAM users and roles</strong>.</li> 
   <li>For <strong>IAM users and roles</strong>, enter <code>Glue-execution-role</code>.</li> 
   <li>Under <strong>LF-Tags or catalog resources</strong>, select <strong>Named Data Catalog resources</strong>.</li> 
   <li>For <strong>Catalogs</strong>, choose <code>rl_link_container_ordersdb</code> and the consumer account ID, which indicates the default catalog.</li> 
   <li>Under <strong>Catalog permissions</strong>, select <strong>Describe</strong> for <strong>Catalog permissions</strong>.</li> 
   <li>Choose <strong>Grant</strong>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/039-BDB-5089.png"><img alt="039-BDB 5089" class="alignnone size-full wp-image-77825" height="672" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/039-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/040-BDB-5089.png"><img alt="040-BDB 5089" class="alignnone size-full wp-image-77824" height="520" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/040-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> 
  <ol start="9"> 
   <li>Repeat these steps for the catalog <code>rl_link_container_ordersdb</code>: 
    <ol type="a"> 
     <li>On the <strong>Databases</strong> dropdown menu, choose <code>public_db</code>.</li> 
     <li>Under <strong>Database permissions</strong>, select <strong>Describe</strong>.</li> 
     <li>Choose <strong>Grant</strong>.</li> 
    </ol> </li> 
   <li>Repeat these steps again, but after choosing <code>rl_link_container_ordersdb</code> and <code>public_db</code>, on the <strong>Tables</strong> dropdown menu, choose <code>rl_orderstbl</code>. 
    <ol type="a"> 
     <li>Under <strong>Resource link permissions</strong>, select <strong>Describe</strong>.</li> 
     <li>Choose <strong>Grant</strong>.</li> 
    </ol> </li> 
   <li>Repeat these steps to grant additional permissions to <code>Glue-execution-role</code>. 
    <ol type="a"> 
     <li>For this iteration, grant <strong>Describe</strong> permissions on the default catalog databases <code>public</code> and <code>customerdb</code>.</li> 
     <li>Grant <strong>Describe</strong> permission on the resource link <code>customerdb</code>.</li> 
     <li>Grant <strong>Select</strong> permission on the tables <code>returnstbl_iceberg</code> and <code>orderstbl</code>.</li> 
    </ol> </li> 
  </ol> <p>The following screenshots show the configuration for database <code>public</code> and <code>customerdb</code> permissions.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/041-BDB-5089.png"><img alt="041-BDB 5089" class="alignnone size-full wp-image-77823" height="964" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/041-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="776" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/042-BDB-5089.png"><img alt="042-BDB 5089" class="alignnone size-full wp-image-77822" height="495" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/042-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1188" /></a></p> <p>The following screenshots show the configuration for resource link <code>customerdb</code> permissions.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/043-BDB-5089.png"><img alt="043-BDB 5089" class="alignnone size-full wp-image-77821" height="962" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/043-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="774" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/044-BDB-5089.png"><img alt="044-BDB 5089" class="alignnone size-full wp-image-77820" height="357" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/044-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1084" /></a></p> <p>The following screenshots show the configuration for table <code>returnstbl_iceberg</code> permissions.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/045-BDB-5089.png"><img alt="045-BDB 5089" class="alignnone size-full wp-image-77819" height="1512" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/045-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1145" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/046-BDB-5089.png"><img alt="046-BDB 5089" class="alignnone size-full wp-image-77818" height="544" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/046-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> <p>The following screenshots show the configuration for table <code>orderstbl</code> permissions.</p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/047-BDB-5089.png"><img alt="047-BDB 5089" class="alignnone size-full wp-image-77817" height="966" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/047-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="772" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/048-BDB-5089.png"><img alt="048-BDB 5089" class="alignnone size-full wp-image-77816" height="544" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/048-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> 
  <ol start="12"> 
   <li>In the navigation pane, choose <strong>Data permissions</strong> and verify permissions on <code>Glue-execution-role</code>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/049-BDB-5089.png"><img alt="049-BDB 5089" class="alignnone size-full wp-image-77815" height="514" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/049-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="936" /></a></p> <h3>Run a PySpark job in AWS Glue 5.0</h3> <p>Download the PySpark script <a href="https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-5089/LakeHouseGlueSparkJob.py" rel="noopener" target="_blank">LakeHouseGlueSparkJob.py</a>. This AWS Glue PySpark script runs Spark SQL by joining the producer shared federated <code>orderstbl</code> table and Amazon S3 based <code>returns</code> table in the consumer account to analyze the data and identify the total orders placed per market.</p> <p>Replace <code>&lt;&lt;consumer_account_id&gt;&gt;</code> in the script with your consumer account ID. Complete the following steps to create and run an AWS Glue job:</p> 
  <ol> 
   <li>On the AWS Glue console, in the navigation pane, choose <strong>ETL jobs</strong>.</li> 
   <li>Choose <strong>Create job</strong>, then choose <strong>Script editor</strong>.</li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/050-BDB-5089.png"><img alt="050-BDB 5089" class="alignnone size-full wp-image-77814" height="189" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/050-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> 
  <ol start="3"> 
   <li>For <strong>Engine</strong>, choose <strong>Spark</strong>.</li> 
   <li>For <strong>Options</strong>, choose<strong> Start fresh</strong>.</li> 
   <li>Choose <strong>Upload script</strong>.</li> 
   <li>Browse to the location where you downloaded and edited the script, select the script, and choose <strong>Open</strong>.</li> 
   <li>On the <strong>Job details</strong> tab, provide the following information: 
    <ol type="a"> 
     <li>For <strong>Name</strong>, enter a name (for this post, <code>LakeHouseGlueSparkJob</code>).</li> 
     <li>Under <strong>Basic properties</strong>, for <strong>IAM role</strong>, choose <code>Glue-execution-role</code>.</li> 
     <li>For <strong>Glue version</strong>, select <strong>Glue 5.0</strong>.</li> 
     <li>Under <strong>Advanced properties</strong>, for <strong>Job parameters</strong>, choose <strong>Add new parameter</strong>.</li> 
     <li>Add the parameters<code> --datalake-formats = iceberg</code> and <code>--enable-lakeformation-fine-grained-access = true</code>.</li> 
    </ol> </li> 
   <li>Save the job.</li> 
   <li>Choose <strong>Run</strong> to execute the AWS Glue job, and wait for the job to complete.</li> 
   <li>Review the job run details from the <strong>Output logs</strong></li> 
  </ol> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/051-BDB-5089.png"><img alt="051-BDB 5089" class="alignnone size-full wp-image-77813" height="592" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/051-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="1292" /></a></p> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/052-BDB-5089.png"><img alt="052-BDB 5089" class="alignnone size-full wp-image-77812" height="322" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/052-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="742" /></a></p> <h2>Clean up</h2> <p>To avoid incurring costs on your AWS accounts, clean up the resources you created:</p> 
  <ol> 
   <li>Delete the Lake Formation permissions, catalog link container, database, and tables in the consumer account.</li> 
   <li>Delete the AWS Glue job in the consumer account.</li> 
   <li>Delete the federated catalog, database, and table resources in the producer account.</li> 
   <li>Delete the Redshift Serverless namespace in the producer account.</li> 
   <li>Delete the S3 buckets you created as part of data transfer in both accounts and the Athena query results bucket in the consumer account.</li> 
   <li>Clean up the IAM roles you created for the SageMaker Lakehouse setup as part of the prerequisites.</li> 
  </ol> <h2>Conclusion</h2> <p>In this post, we illustrated how to bring your existing Redshift tables to SageMaker Lakehouse and share them securely with external AWS accounts. We also showed how to query the shared data warehouse and data lakehouse tables in the same Spark session, from a recipient account, using Spark in AWS Glue 5.0.</p> <p>We hope you find this useful to integrate your Redshift tables with an existing data mesh and access the tables using AWS Glue Spark. Test this solution in your accounts and share feedback in the comments section. Stay tuned for more updates and feel free to explore the features of <a href="https://aws.amazon.com/sagemaker/lakehouse/" rel="noopener" target="_blank">SageMaker Lakehouse</a> and <a href="https://docs.aws.amazon.com/glue/latest/dg/release-notes.html" rel="noopener" target="_blank">AWS Glue versions</a>.</p> <h2>Appendix: Table creation</h2> <p>Complete the following steps to create a returns table in the Amazon S3 based default catalog and an orders table in Amazon Redshift:</p> 
  <ol> 
   <li>Download the CSV format datasets <a href="https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-5089/orders.csv">orders</a> and <a href="https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-5089/returns.csv">returns</a>.</li> 
   <li>Upload them to your S3 bucket under the corresponding table prefix path.</li> 
   <li>Use the following SQL statements in Athena. First-time users of Athena should refer to <a href="https://docs.aws.amazon.com/athena/latest/ug/query-results-specify-location.html" rel="noopener" target="_blank">Specify a query result location</a>.</li> 
  </ol> 
  <div class="hide-language"> 
   <pre><code class="lang-sql">CREATE DATABASE customerdb;
CREATE EXTERNAL TABLE customerdb.returnstbl_csv(
  `returned` string, 
  `order_id` string, 
  `market` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\;' 
LOCATION
  's3://&lt;your-S3-bucket&gt;/&lt;prefix-for-returns-table-data&gt;/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);

select * from customerdb.returnstbl_csv limit 10; 
</code></pre> 
  </div> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/053-BDB-5089.png"><img alt="053-BDB 5089" class="alignnone size-full wp-image-77811" height="472" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/053-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> 
  <ol start="4"> 
   <li>Create an Iceberg format table in the default catalog and insert data from the CSV format table:</li> 
  </ol> 
  <div class="hide-language"> 
   <pre><code class="lang-sql">CREATE TABLE customerdb.returnstbl_iceberg(
  `returned` string, 
  `order_id` string, 
  `market` string)
LOCATION 's3://&lt;your-producer-account-bucket&gt;/returnstbl_iceberg/' 
TBLPROPERTIES (
  'table_type'='ICEBERG'
);

INSERT INTO customerdb.returnstbl_iceberg
SELECT *
FROM returnstbl_csv;  

SELECT * FROM customerdb.returnstbl_iceberg LIMIT 10; 
</code></pre> 
  </div> <p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/054-BDB-5089.png"><img alt="054-BDB 5089" class="alignnone size-full wp-image-77810" height="540" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/054-BDB-5089.png" style="margin: 10px 0px 10px 0px;" width="844" /></a></p> 
  <ol start="5"> 
   <li>To create the orders table in the Redshift Serverless namespace, open the <a href="https://console.aws.amazon.com/redshiftv2/" rel="noopener" target="_blank">Query Editor v2</a> on the Amazon Redshift console.</li> 
   <li>Connect to the default namespace using your database admin user credentials.</li> 
   <li>Run the following commands in the SQL editor to create the database <code>ordersdb</code> and table orderstbl in it. Copy the data from your S3 location of the orders data to the <code>orderstbl</code>:</li> 
  </ol> 
  <div class="hide-language"> 
   <pre><code class="lang-sql">create database ordersdb;
use ordersdb;

create table orderstbl(
  row_id int, 
  order_id VARCHAR, 
  order_date VARCHAR, 
  ship_date VARCHAR, 
  ship_mode VARCHAR, 
  customer_id VARCHAR, 
  customer_name VARCHAR, 
  segment VARCHAR, 
  city VARCHAR, 
  state VARCHAR, 
  country VARCHAR, 
  postal_code int, 
  market VARCHAR, 
  region VARCHAR, 
  product_id VARCHAR, 
  category VARCHAR, 
  sub_category VARCHAR, 
  product_name VARCHAR, 
  sales VARCHAR, 
  quantity bigint, 
  discount VARCHAR, 
  profit VARCHAR, 
  shipping_cost VARCHAR, 
  order_priority VARCHAR
  );

copy orderstbl
from 's3://&lt;your-s3-bucket&gt;/ordersdatacsv/orders.csv' 
iam_role 'arn:aws:iam::&lt;producer-account-id&gt;:role/service-role/&lt;your-Redshift-Role&gt;'
CSV 
DELIMITER ';'
IGNOREHEADER 1
;

select * from ordersdb.orderstbl limit 5;
</code></pre> 
  </div> 
  <div class="hide-language"> 
   <hr /> 
  </div> <h3>About the Authors</h3> <p style="clear: both;"><strong><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/055-BDB-5089.png"><img alt="055-BDB 5089" class="alignleft wp-image-77809 size-thumbnail" height="130" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/055-BDB-5089-100x130.png" width="100" /></a>Aarthi Srinivasan</strong> is a Senior Big Data Architect with Amazon SageMaker Lakehouse. She collaborates with the service team to enhance product features, works with AWS customers and partners to architect lakehouse solutions, and establishes best practices for data governance.</p> <p style="clear: both;"><strong><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/056-BDB-5089.png"><img alt="056-BDB 5089" class="alignleft wp-image-77808 size-thumbnail" height="132" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/056-BDB-5089-100x132.png" width="100" /></a>Subhasis Sarkar</strong> is a Senior Data Engineer with Amazon. Subhasis thrives on solving complex technological challenges with innovative solutions. He specializes in AWS data architectures, particularly data mesh implementations using AWS CDK components.</p> </li> 
</ul>
