---
title: "Build Write-Audit-Publish pattern with Apache Iceberg branching and AWS Glue Data Quality"
url: "https://aws.amazon.com/blogs/big-data/build-write-audit-publish-pattern-with-apache-iceberg-branching-and-aws-glue-data-quality/"
date: "Mon, 09 Dec 2024 22:24:12 +0000"
author: "Tomohiro Tanaka"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>Given the importance of data in the world today, organizations face the dual challenges of managing large-scale, continuously incoming data while vetting its quality and reliability. The importance of publishing only high-quality data can’t be overstated—it’s the foundation for accurate analytics, reliable machine learning (ML) models, and sound decision-making. Equally crucial is the ability to segregate and audit problematic data, not just for maintaining data integrity, but also for regulatory compliance, error analysis, and potential data recovery.</p> 
<p><a href="https://aws.amazon.com/glue/" rel="noopener" target="_blank">AWS Glue</a> is a serverless data integration service that you can use to effectively monitor and manage data quality through <a href="https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html" rel="noopener" target="_blank">AWS Glue Data Quality</a>. Today, many customers build data quality validation pipelines using its <a href="https://docs.aws.amazon.com/glue/latest/dg/dqdl.html" rel="noopener" target="_blank">Data Quality Definition Language</a> (DQDL) because with static rules, <a href="https://docs.aws.amazon.com/glue/latest/dg/dqdl.html#dqdl-dynamic-rules" rel="noopener" target="_blank">dynamic rules</a>, and <a href="https://docs.aws.amazon.com/glue/latest/dg/data-quality-anomaly-detection.html" rel="noopener" target="_blank">anomaly detection capability</a>, it’s fairly straightforward.</p> 
<p><a href="https://iceberg.apache.org/" rel="noopener" target="_blank">Apache Iceberg</a> is an open table format that brings <a href="https://aws.amazon.com/compare/the-difference-between-acid-and-base-database/" rel="noopener" target="_blank">atomicity, consistency, isolation, and durability</a> (ACID) transactions to data lakes, streamlining data management. One of its key features is the ability to manage data using <a href="https://iceberg.apache.org/docs/1.6.1/branching/" rel="noopener" target="_blank">branches</a>. Each branch has its own lifecycle, allowing for flexible and efficient data management strategies.</p> 
<p>This post explores robust strategies for maintaining data quality when ingesting data into Apache Iceberg tables using AWS Glue Data Quality and Iceberg branches. We discuss two common strategies to verify the quality of published data. We dive deep into the Write-Audit-Publish (WAP) pattern, demonstrating how it works with Apache Iceberg.</p> 
<h2>Strategy for managing data quality</h2> 
<p>When it comes to vetting data quality in streaming environments, two prominent strategies emerge: the <a href="https://aws.amazon.com/what-is/dead-letter-queue/" rel="noopener" target="_blank">dead-letter queue</a> (DLQ) approach and the WAP pattern. Each strategy offers unique advantages and considerations.</p> 
<ul> 
 <li><strong>The DLQ approach</strong> – Segregate problematic entries from high-quality data so that only clean data makes it into your primary dataset.</li> 
 <li><strong>The WAP pattern</strong> – Using branches, segregate problematic entries from high-quality data so that only clean data is published in the main branch.</li> 
</ul> 
<h3>The DLQ approach</h3> 
<p>The DLQ strategy focuses on efficiently segregating high-quality data from problematic entries so that only clean data makes it into your primary dataset. Here’s how it works:</p> 
<ol> 
 <li>As data streams in, it passes through a validation process</li> 
 <li>Valid data is written directly to the table referred by downstream users</li> 
 <li>Invalid or problematic data is redirected to a separate DLQ for later analysis and potential recovery</li> 
</ol> 
<p>The following screenshot shows this flow.</p> 
<p><img alt="bdb4341_0_1_dlq" class="alignnone wp-image-71787 size-full" height="270" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/0_1_dlq.png" style="margin: 10px 0px 10px 0px;" width="704" /></p> 
<p>Here are its advantages:</p> 
<ul> 
 <li><strong>Simplicity</strong> – The DLQ approach is straightforward to implement, especially when there is only one writer</li> 
 <li><strong>Low latency</strong> – Valid data is instantly available in the main branch for downstream consumers</li> 
 <li><strong>Separate processing for invalid data</strong> – You can have dedicated jobs to process the DLQ for auditing and recovery purposes.</li> 
</ul> 
<p>The DLQ strategy can present significant challenges in complex data environments. With multiple concurrent writers to the same Iceberg table, maintaining consistent DLQ implementation becomes difficult. This issue is compounded when different engines (for example, Spark, Trino, or Python) are used for writes because the DLQ logic may vary between them, making system maintenance more complex. Additionally, storing invalid data separately can lead to management overhead.</p> 
<p>Additionally, for low-latency requirements, the processing validation step may introduce additional delays. This creates a challenge in balancing data quality with speed of delivery.</p> 
<p>To solve those challenges in a reasonable way, we introduce the WAP pattern in the next section.</p> 
<h3>The WAP pattern</h3> 
<p>The WAP pattern implements a three-stage process:</p> 
<ol> 
 <li><strong>Write</strong> – Data is initially written to a staging branch</li> 
 <li><strong>Audit</strong> – Quality checks are performed on the staging branch</li> 
 <li><strong>Publish</strong> – Validated data is merged into the main branch for consumption</li> 
</ol> 
<p>The following screenshot shows this flow.</p> 
<p><img alt="bdb4341_0_2_wap" class="alignnone wp-image-71788 size-full" height="131" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/0_2_wap.png" style="margin: 10px 0px 10px 0px;" width="1024" /></p> 
<p>Here are its advantages:</p> 
<ul> 
 <li><strong>Flexible data latency management</strong> – In the WAP pattern, the raw data is ingested to the staging branch without data validation, and then the high-quality data is ingested to the main branch with data validation. With this characteristic, there’s flexibility to achieve urgent, low-latency data handling on the staging branch and achieve high-quality data handling on the main branch.</li> 
 <li><strong>Unified data quality management</strong> – The WAP pattern separates the audit and publish logic from the writer applications. It provides a unified approach to quality management, even with multiple writers or varying data sources. The audit phase can be customized and evolved without affecting the write or publish stages.</li> 
</ul> 
<p>The primary challenge of the WAP pattern is the increased latency it introduces. The multistep process inevitably delays data availability for downstream consumers, which may be problematic for near real-time use cases. Furthermore, implementing this pattern requires more sophisticated orchestration compared to the DLQ approach, potentially increasing development time and complexity.</p> 
<h2>How the WAP pattern works with Iceberg</h2> 
<p>The following sections explore how the WAP pattern works with Iceberg.</p> 
<h3>Iceberg’s branching feature</h3> 
<p>Iceberg offers a branching feature for data lifecycle management, which is particularly useful for efficiently implementing the WAP pattern. The metadata of an Iceberg table stores a history of snapshots. These snapshots, created for each change to the table, are fundamental to concurrent access control and table versioning. Branches are independent histories of snapshots branched from another branch, and each branch can be referred to and updated separately.</p> 
<p>When a table is created, it starts with only a main branch, and all transactions are initially written to it. You can create additional branches, such as an audit branch, and configure engines to write to them. Changes on one branch can be fast-forwarded to another branch using Spark’s <code>fast_forward</code> procedure, as shown in the following screenshot.</p> 
<p><img alt="bdb4341_0_3_iceberg-branch" class="alignnone wp-image-71789 size-full" height="211" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/0_3_iceberg-branch.png" style="margin: 10px 0px 10px 0px;" width="781" /></p> 
<h3>How to manage Iceberg branches</h3> 
<p>In this section, we cover the essential operations for managing Iceberg branches using SparkSQL. We’ll demonstrate how to use the branches, specifically, to create a new branch, write to and read from a specific branch, and set a default branch for a Spark session. These operations form the foundation for implementing the WAP pattern with Iceberg.</p> 
<p>To create a branch, run the following SparkSQL query:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">ALTER TABLE glue_catalog.db.tbl CREATE BRANCH audit</code></pre> 
</div> 
<p>To specify a branch to be updated, use the <code>glue_catalog.&lt;database_name&gt;.&lt;table_name&gt;.branch_&lt;branch_name&gt;</code> syntax:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">INSERT INTO glue_catalog.db.tbl.branch_audit VALUES (1, 'a'), (2, 'b');</code></pre> 
</div> 
<p>To specify a branch to be queried, use the <code>glue_catalog.&lt;database_name&gt;.&lt;table_name&gt;.branch_&lt;branch_name&gt;</code> syntax:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">SELECT * FROM glue_catalog.db.tbl.branch_audit;</code></pre> 
</div> 
<p>To specify a branch for the entire Spark session scope, set the branch name to the Spark parameter spark.wap.branch. After this parameter is set, all queries will refer to the specified branch without explicit expression:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">SET spark.wap.branch = audit

-- audit branch will be updated
INSERT INTO glue_catalog.db.tbl VALUES (3, 'c');</code></pre> 
</div> 
<h3>How to implement the WAP pattern with Iceberg branches</h3> 
<p>Using Iceberg’s branching feature, we can efficiently implement the WAP pattern with a single Iceberg table. Additionally, Iceberg characteristics such as ACID transactions and schema evolution are useful for handling multiple concurrent writers and varying data.</p> 
<ol> 
 <li><strong>Write –</strong> The data ingestion process switches branch from main and it commits updates to the audit branch, instead of the main branch. At this point, these updates aren’t accessible to downstream users who can only access the main branch.</li> 
 <li><strong>Audit – </strong>The audit process runs data quality checks on the data in the audit branch. It specifies which data is clean and ready to be provided.</li> 
 <li><strong>Publish – </strong>The audit process publishes validated data to the main branch with the Iceberg <code>fast_forward</code> procedure, making it available for downstream users.</li> 
</ol> 
<p>This flow is shown in the following screenshot.</p> 
<p><img alt="bdb4341_0_4_wap-w-iceberg-branch" class="alignnone wp-image-71790 size-full" height="381" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/0_4_wap-w-iceberg-branch.png" style="margin: 10px 0px 10px 0px;" width="914" /></p> 
<p>By implementing the WAP pattern with Iceberg, we can obtain several advantages:</p> 
<ul> 
 <li><strong>Simplicity –</strong> Iceberg branches can express multiple states of a table, such as audit and main, within one table. We can have unified data management even when handling multiple data contexts separately and uniformly.</li> 
 <li><strong>Handling concurrent writers –</strong> Iceberg tables are ACID compliant, so consistent reads and writes are guaranteed even when multiple reader and writer processes run concurrently.</li> 
 <li><strong>Schema evolution –</strong> If there are issues with the data being ingested, its schema may differ from the table definition. Spark supports dynamic schema merging for Iceberg tables. Iceberg tables can flexibly evolve their schema to write data with inconsistent schemas. By configuring the following parameters, when schema changes occur, new columns from the source are added to the target table with NULL values for existing rows. Columns present only in the target have their values set to NULL for new insertions or left unchanged during updates.</li> 
</ul> 
<div class="hide-language"> 
 <pre><code class="lang-python">SET `spark.sql.iceberg.check-ordering` = false

ALTER TABLE glue_catalog.db.tbl SET TBLPROPERTIES (
&nbsp;&nbsp;&nbsp; 'write.spark.accept-any-schema'='true'
)
df.writeTo("glue_catalog.db.tbl").option("merge-schema","true").append()</code></pre> 
</div> 
<p>As an intermediate wrap-up, the WAP pattern offers a robust approach to managing the balance between data quality and latency. With Iceberg branches, we can implement WAP pattern simply on single Iceberg table with handling concurrent writers and schema evolution.</p> 
<h2>Example use case</h2> 
<p>Suppose that a home monitoring system tracks room temperature and humidity. The system captures and sends the data to an Iceberg based data lake built on top of <a href="https://aws.amazon.com/s3/" rel="noopener" target="_blank">Amazon Simple Storage Service</a> (Amazon S3). The data is visualized using matplotlib for interactive data analysis. For the system, issues such as device malfunctions or network problems can lead to partial or erroneous data being written, resulting in incorrect insights. In many cases, these issues are only detected after the data is sent to the data lake. Additionally, the correctness of such data is generally complicated.</p> 
<p>To address these issues, the WAP pattern using Iceberg branches is applied for the system in this post. Through this approach, the incoming room data to the data lake is evaluated for quality before being visualized, and you make sure that only qualified room data is used for further data analysis. With the WAP pattern using the branches, you can achieve effective data management and promote data quality in downstream processes. The solution is demonstrated using AWS Glue Studio notebook, which is a managed Jupyter Notebook for interacting with Apache Spark.</p> 
<h2>Prerequisites</h2> 
<p>The following prerequisites are necessary for this use case:</p> 
<ul> 
 <li>An active AWS Account that provides access to AWS Glue, Amazon S3 and <a href="https://aws.amazon.com/cloudformation/" rel="noopener" target="_blank">AWS CloudFormation</a>.</li> 
 <li>Permissions to create and deploy AWS CloudFormation For instructions, see <a href="https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/stacksets-getting-started-create.html" rel="noopener" target="_blank">Create a stack set using the CloudFormation console or AWS CLI</a>.</li> 
</ul> 
<h2>Set up resources with AWS CloudFormation</h2> 
<p>First, you use a provided AWS CloudFormation template to set up resources to build Iceberg environments. The template creates the following resources:</p> 
<ul> 
 <li>An S3 bucket for metadata and data files of an Iceberg table</li> 
 <li>A database for the Iceberg table in AWS Glue Data Catalog</li> 
 <li>An <a href="https://aws.amazon.com/iam/" rel="noopener" target="_blank">AWS Identity and Access Management</a> (IAM) role for an AWS Glue job</li> 
</ul> 
<p>Complete the following steps to deploy the resources.</p> 
<ol> 
 <li>Choose <strong>Launch stack</strong>.</li> 
</ol> 
<p><a href="https://console.aws.amazon.com/cloudformation/home?#/stacks/new?templateURL=https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-4341/WAP.yaml&amp;stackName=wap" rel="noopener" target="_blank"><img alt="Launch Button" class="alignnone wp-image-3705 size-full" height="20" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2017/11/15/LaunchStack.png" width="107" /></a></p> 
<ol start="2"> 
 <li>For the <strong>Parameters</strong>, <strong>IcebergDatabaseName</strong> is set by default. You can also change the default value. Then, choose <strong>Next</strong>.</li> 
 <li>Choose <strong>Next</strong>.</li> 
 <li>Choose <strong>I acknowledge that AWS CloudFormation might create IAM resources with custom names</strong>.</li> 
 <li>Choose <strong>Submit</strong>.</li> 
 <li>After the stack creation is complete, check the <strong>Outputs</strong> The resource values are used in the following sections.</li> 
</ol> 
<p>Next, configure the Iceberg JAR files to the session to use the Iceberg branch feature. Complete the following steps:</p> 
<ol> 
 <li>Select the following JAR files from the <a href="https://iceberg.apache.org/releases/" rel="noopener" target="_blank">Iceberg releases page</a> and download these JAR files on your local machine: 
  <ol type="a"> 
   <li><strong>1.6.1 Spark 3.3_with Scala 2.12 runtime Jar</strong></li> 
   <li><strong>1.6.1 aws-bundle Jar</strong></li> 
  </ol> </li> 
 <li>Open the <a href="https://console.aws.amazon.com/s3/buckets" rel="noopener" target="_blank">Amazon S3 console</a> and select the S3 bucket you created through the CloudFormation stack. The S3 bucket name can be found on the CloudFormation <strong>Outputs</strong> tab.<strong><br /> </strong></li> 
 <li>Choose <strong>Create folder</strong> and create the jars path in the S3 bucket.</li> 
 <li>Upload the two downloaded JAR files to <code>s3://&lt;IcebergS3Bucket&gt;/jars/</code> from the S3 console.</li> 
</ol> 
<h2>Upload a Jupyter Notebook on AWS Glue Studio</h2> 
<p>After launching the CloudFormation stack, you create an AWS Glue Studio notebook to use Iceberg with AWS Glue. Complete the following steps.</p> 
<ol> 
 <li>Download <a href="https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-4341/wap.ipynb" rel="noopener" target="_blank">wap.ipynb</a>.</li> 
 <li>Open <a href="https://console.aws.amazon.com/gluestudio/home#/jobs" rel="noopener" target="_blank">AWS Glue Studio console</a>.</li> 
 <li>Under <strong>Create job</strong>, select <strong>Notebook</strong>.</li> 
 <li>Select <strong>Upload Notebook</strong>, choose <strong>Choose file</strong>, and upload the notebook you downloaded.</li> 
 <li>Select the IAM role name, such as <strong>IcebergWAPGlueJobRole</strong>, that you created through the CloudFormation stack. Then, choose <strong>Create notebook</strong>.</li> 
 <li>For <strong>Job name</strong> at the left top of the page, enter <code>iceberg_wap</code>.</li> 
 <li>Choose <strong>Save</strong>.</li> 
</ol> 
<h2>Configure Iceberg branches</h2> 
<p>Start by creating an Iceberg table that contains a room temperature and humidity dataset. After creating the Iceberg table, create branches that are used for performing the WAP practice. Complete the following steps:</p> 
<ol> 
 <li>On the Jupyter Notebook that you created in <strong>Upload a Jupyter Notebook on AWS Glue Studio</strong>, run the following cell to use Iceberg with Glue. <code>%additional_python_modules pandas==2.2</code> is used to visualize the temperature and humidity data in the notebook with pandas. Before running the cell, replace <code>&lt;IcebergS3Bucket&gt;</code> with the S3 bucket name where you uploaded the Iceberg JAR files.</li> 
</ol> 
<p><img alt="bdb4341_1_session-config" class="alignnone wp-image-71792 size-full" height="442" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/1_session-config.png" style="margin: 10px 0px 10px 0px;" width="2230" /></p> 
<ol start="2"> 
 <li>Initialize the SparkSession by running the following cell. The first three settings, starting with <code>spark.sql</code>, are required to use Iceberg with Glue. The default catalog name is set to <code>glue_catalog</code> using <code>spark.sql.defaultCatalog</code>. The configuration <code>spark.sql.execution.arrow.pyspark.enabled</code> is set to <code>true</code> and is used for data visualization with pandas.</li> 
</ol> 
<p><img alt="bdb4341_2_sparksession-init" class="alignnone wp-image-71793 size-full" height="510" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/2_sparksession-init.png" style="margin: 10px 0px 10px 0px;" width="1816" /></p> 
<ol start="3"> 
 <li>After the session is created (the notification <code>Session &lt;Session Id&gt; has been created.</code> will be displayed in the notebook), run the following commands to copy the temperature and humidity dataset to the S3 bucket you created through the CloudFormation stack. Before running the cell, replace <code>&lt;IcebergS3Bucket&gt;</code> with the name of the S3 bucket for Iceberg, which you can find on the CloudFormation <strong>Outputs</strong> tab.<strong><br /> </strong></li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">!aws s3 cp s3://aws-blogs-artifacts-public/artifacts/BDB-4341/data/part-00000-fa08487a-43c2-4398-bae9-9cb912f8843c-c000.snappy.parquet s3://&lt;IcebergS3Bucket&gt;/src-data/current/&nbsp;
!aws s3 cp s3://aws-blogs-artifacts-public/artifacts/BDB-4341/data/new-part-00000-e8a06ab0-f33d-4b3b-bd0a-f04d366f067e-c000.snappy.parquet s3://&lt;IcebergS3Bucket&gt;/src-data/new/</code></pre> 
</div> 
<ol start="4"> 
 <li>Configure the data source bucket name and path (<code>DATA_SRC</code>), Iceberg data warehouse path (<code>ICEBERG_LOC</code>), and database and table names for an Iceberg table (<code>DB_TBL</code>). Replace <code>&lt;IcebergS3Bucket&gt;</code> with the S3 bucket from the CloudFormation <strong>Outputs</strong> tab.<strong><br /> </strong></li> 
 <li>Read the dataset and create the Iceberg table with the dataset using the Create Table As Select (CTAS) query.</li> 
</ol> 
<p><img alt="bdb4341_3_ctas" class="alignnone wp-image-71794 size-full" height="404" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/3_ctas.png" style="margin: 10px 0px 10px 0px;" width="1430" /></p> 
<ol start="6"> 
 <li>Run the following code to display the temperature and humidity data for each room in the Iceberg table. Pandas and matplotlib are used to visualize the data for each room. The data from 10:05 to 10:30 is displayed in the notebook, as shown in the following screenshot, with each room showing approximately 25°C for temperature (displayed as the blue line) and 52% for humidity (displayed as the orange line).</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-python">import matplotlib.pyplot as plt
import pandas as pd

CONF = [
&nbsp;&nbsp; &nbsp;{'room_type': 'myroom', 'cols':['current_temperature', 'current_humidity']},
&nbsp;&nbsp; &nbsp;{'room_type': 'living', 'cols':['current_temperature', 'current_humidity']},
&nbsp;&nbsp; &nbsp;{'room_type': 'kitchen', 'cols':['current_temperature', 'current_humidity']}
]

fig, axes = plt.subplots(nrows=3, ncols=1, sharex=True, sharey=True)
for ax, conf in zip(axes.ravel(), CONF):
&nbsp;&nbsp; &nbsp;df_room = spark.sql(f"""
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;SELECT current_time, current_temperature, current_humidity, room_type
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;FROM {DB_TBL} WHERE room_type = '{conf['room_type']}'
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;ORDER BY current_time ASC
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;""")
&nbsp;&nbsp; &nbsp;pdf = df_room.toPandas()
&nbsp;&nbsp; &nbsp;pdf.set_index(pdf['current_time'], inplace=True)
&nbsp;&nbsp; &nbsp;plt.xlabel('time')
&nbsp;&nbsp; &nbsp;plt.ylabel('temperature/humidity')
&nbsp;&nbsp; &nbsp;plt.ylim(10, 60)
&nbsp;&nbsp; &nbsp;plt.yticks([tick for tick in range(10, 60, 10)])
&nbsp;&nbsp; &nbsp;pdf[conf['cols']].plot.line(ax=ax, grid=True, figsize=(8, 6), title=conf['room_type'], legend=False, marker=".", markersize=2, linewidth=0)

plt.legend(['temperature', 'humidity'], loc='center', bbox_to_anchor=(0, 1, 1, 5.5), ncol=2)

%matplot plt</code></pre> 
</div> 
<p><img alt="bdb4341_4_vis-1" class="alignnone wp-image-71795 size-full" height="1074" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/4_vis-1.png" style="margin: 10px 0px 10px 0px;" width="1370" /></p> 
<ol start="7"> 
 <li>You create Iceberg branches by running the following queries before writing data into the Iceberg table. You can create an Iceberg branch by the <code>ALTER TABLE db.table CREATE BRANCH &lt;branch_name&gt;</code> query.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-sql">ALTER TABLE iceberg_wap_db.room_data CREATE BRANCH stg
ALTER TABLE iceberg_wap_db.room_data CREATE BRANCH audit</code></pre> 
</div> 
<p>Now, you’re ready to build the WAP pattern with Iceberg.</p> 
<h2>Build WAP pattern with Iceberg</h2> 
<p>Use the Iceberg branches created earlier to implement the WAP pattern. You start writing the newly incoming temperature and humidity data including erroneous values to the <code>stg</code> branch in the Iceberg table.</p> 
<h3>Write phase: Write incoming data into the Iceberg <code>stg</code> branch</h3> 
<p>To write the incoming data into the <code>stg</code> branch in the Iceberg table, complete the following steps:</p> 
<ol> 
 <li>Run the following cell and write the data into Iceberg table.</li> 
</ol> 
<p><img alt="bdb4341_5_write" class="alignnone wp-image-71796 size-full" height="170" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/5_write.png" style="margin: 10px 0px 10px 0px;" width="1780" /></p> 
<ol start="2"> 
 <li>After the records are written, run the following code to visualize the current temperature and humidity data in the <code>stg</code> On the following screenshot, notice that new data was added after 10:30. The output shows incorrect readings, such as around 100°C for temperature between 10:35 and 10:52 in the living room.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-python">fig, axes = plt.subplots(nrows=3, ncols=1, sharex=True, sharey=True)
for ax, conf in zip(axes.ravel(), CONF):
&nbsp;&nbsp; &nbsp;df_room_stg = spark.sql(f"""
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;SELECT current_time, current_temperature, current_humidity, room_type
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;FROM {DB_TBL}.branch_stg WHERE room_type = '{conf['room_type']}'
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;ORDER BY current_time ASC
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;""")
&nbsp;&nbsp; &nbsp;pdf = df_room_stg.toPandas()
&nbsp;&nbsp; &nbsp;pdf.set_index(pdf['current_time'], inplace=True)
&nbsp;&nbsp; &nbsp;plt.xlabel('time')
&nbsp;&nbsp; &nbsp;plt.ylabel('temperature/humidity')
&nbsp;&nbsp; &nbsp;plt.ylim(10, 110)
&nbsp;&nbsp; &nbsp;plt.yticks([tick for tick in range(10, 110, 30)])
&nbsp;&nbsp; &nbsp;pdf[conf['cols']].plot.line(ax=ax, grid=True, figsize=(8, 6), title=conf['room_type'], legend=False, marker=".", markersize=2, linewidth=0)

plt.legend(['temperature', 'humidity'], loc='center', bbox_to_anchor=(0, 1, 1, 5.5), ncol=2)

%matplot plt</code></pre> 
</div> 
<p><img alt="bdb4341_6_vis-2" class="alignnone wp-image-71797 size-full" height="1078" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/6_vis-2.png" style="margin: 10px 0px 10px 0px;" width="1378" /></p> 
<p>The new temperature data including erroneous records was written to the <code>stg</code> branch. This data isn’t visible to the downstream side because it hasn’t been published to the main branch. Next, you evaluate the data quality in the <code>stg</code> branch.</p> 
<h3>Audit phase: Evaluate the data quality in the <code>stg</code> branch</h3> 
<p>In this phase, you evaluate the quality of the temperature and humidity data in the <code>stg</code> branch using AWS Glue Data Quality. Then, the data that doesn’t meet the criteria is filtered out based on the data quality rules, and the qualified data is used to update the latest snapshot in the <code>audit</code> branch. Start with the data quality evaluation:</p> 
<ol> 
 <li>Run the following code to evaluate the current data quality using AWS Glue Data Quality. The evaluation rule is defined in <code>DQ_RULESET</code>, where the normal temperature range is set between −10 and 50°C based on the device specifications. Any values out of this range are considered erroneous in this scenario.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-python">from awsglue.context import GlueContext
from awsglue.transforms import SelectFromCollection
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
DQ_RULESET = """Rules = [ ColumnValues "current_temperature" between -10 and 50 ]"""


dyf = DynamicFrame.fromDF(
&nbsp;&nbsp; &nbsp;dataframe=spark.sql(f"SELECT * FROM {DB_TBL}.branch_stg"),
&nbsp;&nbsp; &nbsp;glue_ctx=GlueContext(spark.sparkContext),
&nbsp;&nbsp; &nbsp;name='dyf')

dyfc_eval_dq = EvaluateDataQuality().process_rows(
&nbsp;&nbsp; &nbsp;frame=dyf,
&nbsp;&nbsp; &nbsp;ruleset=DQ_RULESET,
&nbsp;&nbsp; &nbsp;publishing_options={
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"dataQualityEvaluationContext": "dyfc_eval_dq",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"enableDataQualityCloudWatchMetrics": False,
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"enableDataQualityResultsPublishing": False,
&nbsp;&nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp;additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Show DQ results
dyfc_rule_outcomes = SelectFromCollection.apply(
&nbsp;&nbsp; &nbsp;dfc=dyfc_eval_dq,
&nbsp;&nbsp; &nbsp;key="ruleOutcomes")
dyfc_rule_outcomes.toDF().select('Outcome', 'FailureReason').show(truncate=False)</code></pre> 
</div> 
<ol start="2"> 
 <li>The output shows the result of the evaluation. It displays Failed because some temperature data, such as 105°C, is out of the normal temperature range of −10 to 50°C.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">+-------+------------------------------------------------------+
|Outcome|FailureReason &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
+-------+------------------------------------------------------+
|Failed |Value: 105.0 does not meet the constraint requirement!|
+-------+------------------------------------------------------+</code></pre> 
</div> 
<ol start="3"> 
 <li>After the evaluation, filter out the incorrect temperature data in the <code>stg</code> branch, then update the latest snapshot in the <code>audit</code> branch with the valid temperature data.</li> 
</ol> 
<p><img alt="bdb4341_7_write-to-audit" class="alignnone wp-image-71798 size-full" height="346" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/7_write-to-audit.png" style="margin: 10px 0px 10px 0px;" width="2418" /></p> 
<p>Through the data quality evaluation, the <code>audit</code> branch in the Iceberg table now contains the valid data, which is ready for downstream use.</p> 
<h3>Publish phase: Publish the valid data to the downstream side</h3> 
<p>To publish the valid data in the audit branch to main, complete the following steps:</p> 
<ol> 
 <li>Run the <code>fast_forward</code> Iceberg procedure to publish the valid data in the audit branch to the downstream side.</li> 
</ol> 
<p><img alt="bdb4341_8_publish" class="alignnone wp-image-71799 size-full" height="162" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/8_publish.png" style="margin: 10px 0px 10px 0px;" width="1954" /></p> 
<ol start="2"> 
 <li>After the procedure is complete, review the published data by querying the main branch in the Iceberg table to simulate the query from the downstream side.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-python">fig, axes = plt.subplots(nrows=3, ncols=1, sharex=True, sharey=True)
for ax, conf in zip(axes.ravel(), CONF):
&nbsp;&nbsp; &nbsp;df_room_main = spark.sql(f"""
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;SELECT current_time, current_temperature, current_humidity, room_type
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;FROM {DB_TBL} WHERE room_type = '{conf['room_type']}'
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;ORDER BY current_time ASC
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;""")
&nbsp;&nbsp; &nbsp;pdf = df_room_main.toPandas()
&nbsp;&nbsp; &nbsp;pdf.set_index(pdf['current_time'], inplace=True)
&nbsp;&nbsp; &nbsp;plt.xlabel('time')
&nbsp;&nbsp; &nbsp;plt.ylabel('temperature/humidity')
&nbsp;&nbsp; &nbsp;plt.ylim(10, 60)
&nbsp;&nbsp; &nbsp;plt.yticks([tick for tick in range(10, 60, 10)])
&nbsp;&nbsp; &nbsp;pdf[conf['cols']].plot.line(ax=ax, grid=True, figsize=(8, 6), title=conf['room_type'], legend=False, marker=".", markersize=2, linewidth=0)

plt.legend(['temperature', 'humidity'], loc='center', bbox_to_anchor=(0, 1, 1, 5.5), ncol=2)

%matplot plt</code></pre> 
</div> 
<p>The query result shows only the valid temperature and humidity data that has passed the data quality evaluation.</p> 
<p><img alt="bdb4341_9_vis-3" class="alignnone wp-image-71800 size-full" height="1090" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/9_vis-3.png" style="margin: 10px 0px 10px 0px;" width="1338" /></p> 
<p>In this scenario, you successfully managed data quality by applying the WAP pattern with Iceberg branches. The room temperature and humidity data, including any erroneous records, was first written to the staging branch for quality evaluation. This approach prevented erroneous data from being visualized and leading to incorrect insights. After the data was validated by AWS Glue Data Quality, only valid data was published to the main branch and visualized in the notebook. Using the WAP pattern with Iceberg branches, you can make sure that only validated data is passed to the downstream side for further analysis.</p> 
<h2>Clean up resources</h2> 
<p>To clean up the resources, complete the following steps:</p> 
<ol> 
 <li>On the <a href="https://console.aws.amazon.com/s3/buckets" rel="noopener" target="_blank">Amazon S3 console</a>, select the S3 bucket <code>aws-glue-assets-&lt;ACCOUNT_ID&gt;-&lt;REGION&gt;</code> where the Notebook file (<code>iceberg_wap.ipynb</code>) is stored. Delete the Notebook file located in the <code>notebook</code> path.</li> 
 <li>Select the S3 bucket you created through the CloudFormation template. You can obtain the bucket name from <code>IcebergS3Bucket</code> key on the CloudFormation <strong>Outputs</strong> tab. After selecting the bucket, choose <strong>Empty</strong> to delete all objects.</li> 
 <li>After you confirm the bucket is empty, delete the CloudFormation stack <code>iceberg-wap-baseline-resources</code>.</li> 
</ol> 
<h2>Conclusion</h2> 
<p>In this post, we explored common strategies for maintaining data quality when ingesting data into Apache Iceberg tables. The step-by-step instructions demonstrated how to implement the WAP pattern with Iceberg branches. For use cases requiring data quality validation, the WAP pattern provides the flexibility to manage data latency even with concurrent writer applications without impacting downstream applications.</p> 
<hr /> 
<h3>About the Authors</h3> 
<p style="clear: both;"><img alt="" class="size-full wp-image-30835 alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2022/06/13/tomtan.jpg" width="100" /><strong>Tomohiro Tanaka</strong> is a Senior Cloud Support Engineer at Amazon Web Services. He’s passionate about helping customers use Apache Iceberg for their data lakes on AWS. In his free time, he enjoys a coffee break with his colleagues and making coffee at home.</p> 
<p style="clear: both;"><strong><img alt="" class="size-full wp-image-65588 alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/07/08/sotaro.png" width="100" />Sotaro Hikita</strong> is a Solutions Architect. He supports customers in a wide range of industries, especially the financial industry, to build better solutions. He is particularly passionate about big data technologies and open source software.</p> 
<p style="clear: both;"><img alt="" class="size-full wp-image-16628 alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2021/02/10/Noritaka-Sekiyama-p.png" width="100" /><strong>Noritaka Sekiyama</strong> is a Principal Big Data Architect on the AWS Glue team. He works based in Tokyo, Japan. He is responsible for building software artifacts to help customers. In his spare time, he enjoys cycling with his road bike.</p>
