---
title: "Implement historical record lookup and Slowly Changing Dimensions Type-2 using Apache Iceberg"
url: "https://aws.amazon.com/blogs/big-data/implement-historical-record-lookup-and-slowly-changing-dimensions-type-2-using-apache-iceberg/"
date: "Mon, 09 Dec 2024 22:21:41 +0000"
author: "Tomohiro Tanaka"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>In today’s data-driven world, tracking and analyzing changes over time has become essential. As organizations process vast amounts of data, maintaining an accurate historical record is crucial.&nbsp;History management in data systems is fundamental for compliance, business intelligence, data quality, and time-based analysis. It enables organizations to maintain audit trails, perform trend analysis, identify data quality issues, and conduct point-in-time reporting. When combined with Change Data Capture (CDC), which identifies and captures database changes, history management becomes even more potent.</p> 
<p>Common use cases for historical record management in CDC scenarios span various domains. In customer relationship management, it tracks changes in customer information over time. Financial systems use it for maintaining accurate transaction and balance histories. Inventory management benefits from historical data for analyzing sales patterns and optimizing stock levels. HR systems use it to track employee information changes. In fraud detection, historical data helps identify anomalous patterns in transactions or user behaviors.</p> 
<p>This post will explore how to implement these functionalities using <a href="https://iceberg.apache.org/" rel="noopener" target="_blank">Apache Iceberg</a>, focusing on Slowly Changing Dimensions (SCD) Type-2. This method creates new records for each data change while preserving old ones, thus maintaining a full history. By the end, you’ll understand how to use Apache Iceberg to manage historical records effectively on a typical CDC architecture.</p> 
<h2>Historical record lookup</h2> 
<p>How can we retrieve the history of given records? This is a fundamental question in data management, especially when dealing with systems that need to track changes over time. Let’s explore this concept with a practical example.</p> 
<p>Consider a product (<code>Heater</code>) in an ecommerce database:</p> 
<table border="1px" cellpadding="10px"> 
 <tbody> 
  <tr style="background-color: #000000;"> 
   <td width="88"><span style="color: #ffffff;">product_id</span></td> 
   <td width="107"><span style="color: #ffffff;">product_name</span></td> 
   <td width="88"><span style="color: #ffffff;">price</span></td> 
  </tr> 
  <tr> 
   <td width="88">00001</td> 
   <td width="107">Heater</td> 
   <td width="88">250</td> 
  </tr> 
 </tbody> 
</table> 
<p>Now, let’s say we update the price of this product from <code>250</code> to <code>500</code>. After some time, we want to retrieve the price history of this heater. In a traditional database setup, this task could be challenging, especially if we haven’t explicitly designed our system to track historical changes.</p> 
<p>This is where the concept of historical record lookup becomes crucial. We need a system that not only stores the current state of our data but also maintains a log of all changes made to each record over time. This allows us to answer questions like:</p> 
<ul> 
 <li>What was the price of the heater at a specific point in time?</li> 
 <li>How many times has the price changed, and when did these changes occur?</li> 
 <li>What was the price trend of the heater over the past year?</li> 
</ul> 
<p>Implementing such a system can be complex, requiring careful consideration of data storage, retrieval mechanisms, and query optimization. This is where Apache Iceberg comes into play, offering a feature known as the <a href="https://iceberg.apache.org/docs/latest/spark-procedures/#change-data-capture" rel="noopener" target="_blank">change log view</a>.</p> 
<p>The change log view in Apache Iceberg provides a view of all changes made to a table over time, making it straightforward to query and analyze the history of any record. With change log view, we can easily track insertions, updates, and deletions, giving us a complete picture of how our data has evolved.</p> 
<p>For our heater example, Iceberg’s change log view would allow us to effortlessly retrieve a timeline of all price changes, complete with timestamps and other relevant metadata, as shown in the following table.</p> 
<table border="1px" cellpadding="10px"> 
 <tbody> 
  <tr style="background-color: #000000;"> 
   <td width="88"><span style="color: #ffffff;">product_id</span></td> 
   <td width="107"><span style="color: #ffffff;">product_name</span></td> 
   <td width="88"><span style="color: #ffffff;">price</span></td> 
   <td width="134"><span style="color: #ffffff;">_change_type</span></td> 
  </tr> 
  <tr> 
   <td width="88">00001</td> 
   <td width="107">Heater</td> 
   <td width="88">250</td> 
   <td width="134">INSERT</td> 
  </tr> 
  <tr> 
   <td width="88">00001</td> 
   <td width="107">Heater</td> 
   <td width="88">250</td> 
   <td width="134">UPDATE_BEFORE</td> 
  </tr> 
  <tr> 
   <td width="88">00001</td> 
   <td width="107">Heater</td> 
   <td width="88">500</td> 
   <td width="134">UPDATE_AFTER</td> 
  </tr> 
 </tbody> 
</table> 
<p>This capability not only simplifies historical analysis but also opens possibilities for advanced time-based analytics, auditing, and data governance.</p> 
<h2>Historical table lookup with SCD Type-2</h2> 
<p>SCD Type-2 is a key concept in data warehousing and historical data management and is particularly relevant to Change Data Capture (CDC) scenarios. SCD Type-2 creates new rows for changed data instead of overwriting existing records, allowing for comprehensive tracking of changes over time.</p> 
<p>SCD Type-2 requires additional fields such as <code>effective_start_date</code>, <code>effective_end_date</code>, and <code>current_flag</code> to manage historical records. This approach has been widely used in data warehouses to track changes in various dimensions such as customer information, product details, and employee data. In the example of the previous section, here’s what the SCD Type-2 looks like assuming the update operation is performed on December 11, 2024.</p> 
<table border="1px" cellpadding="10px"> 
 <tbody> 
  <tr style="background-color: #000000;"> 
   <td width="88"><span style="color: #ffffff;">product_id</span></td> 
   <td width="107"><span style="color: #ffffff;">product_name</span></td> 
   <td width="40"><span style="color: #ffffff;">price</span></td> 
   <td width="134"><span style="color: #ffffff;">effective_start_date</span></td> 
   <td width="127"><span style="color: #ffffff;">effective_end_date</span></td> 
   <td width="88"><span style="color: #ffffff;">current_flag</span></td> 
  </tr> 
  <tr> 
   <td width="88">00001</td> 
   <td width="107">Heater</td> 
   <td width="40">250</td> 
   <td width="134">2024-12-10</td> 
   <td width="127">2024-12-11</td> 
   <td width="88">FALSE</td> 
  </tr> 
  <tr> 
   <td width="88">00001</td> 
   <td width="107">Heater</td> 
   <td width="40">500</td> 
   <td width="134">2024-12-11</td> 
   <td width="127">NULL</td> 
   <td width="88">TRUE</td> 
  </tr> 
 </tbody> 
</table> 
<p>SCD Type-2 is particularly valuable in CDC use cases, where capturing all data changes over time is crucial. It enables point-in-time analysis, provides detailed audit trails, aids in data quality management, and helps meet compliance requirements by preserving historical data.</p> 
<p>In traditional implementations on data warehouses, SCD Type-2 requires its specific handling in all <code>INSERT</code>, <code>UPDATE</code>, and <code>DELETE</code> operations that affect those additional columns. For example, to update the price of the product, you need to run the following query.</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">UPDATE product SET effective_end_date = '2024-12-11', current_flag = false
WHERE product_id = '00001' AND current_flag = true;

INSERT INTO product (product_id, product_name, price, effective_start_date, effective_end_date, current_flag)
VALUES ('00001', 'Heater', 500, '2024-12-11', NULL, true);</code></pre> 
</div> 
<p>For modern data lakes, we propose a new approach to implement SCD Type-2. With Iceberg, you can create a dedicated view of SCD Type-2 on top of the change log view, eliminating the need to implement specific handling to make changes on SCD Type-2 tables. With this approach, you can keep managing Iceberg tables without complexity considering SCD Type-2 specification. Anytime when you need SCD Type-2 snapshot of your Iceberg table, you can create the corresponding representation. This approach combines the power of Iceberg’s efficient data management with the historical tracking capabilities of SCD Type-2. By using the change log view, Iceberg can dynamically generate the SCD Type-2 structure without the overhead of maintaining additional tables or manually managing effective dates and flags.</p> 
<p>This streamlined method not only makes the implementation of SCD Type-2 more straightforward, but also offers improved performance and scalability for handling large volumes of historical data in CDC scenarios. It represents a significant advancement in historical data management, merging traditional data warehousing concepts with modern big data capabilities.</p> 
<p>As we delve deeper into Iceberg’s features, we’ll explore how this approach can be implemented, showcasing the efficiency and flexibility it brings to historical data analysis and CDC processes.</p> 
<h2>Prerequisites</h2> 
<p>The following prerequisites are required for the use cases:</p> 
<ul> 
 <li>An active AWS Account that provides access to <a href="https://aws.amazon.com/glue" rel="noopener" target="_blank">AWS Glue</a>, <a href="https://aws.amazon.com/s3" rel="noopener" target="_blank">Amazon Simple Storage Service (Amazon S3)</a> and <a href="https://aws.amazon.com/cloudformation" rel="noopener" target="_blank">AWS CloudFormation</a>.</li> 
 <li>Permissions to create and deploy AWS CloudFormation stacks. For instructions, see Create a stack set using the CloudFormation console or AWS CLI.</li> 
</ul> 
<h2>Set up resources with AWS CloudFormation</h2> 
<p>Use a provided AWS CloudFormation template to set up resources to build Iceberg environments. The template creates the following resources:</p> 
<ul> 
 <li>An S3 bucket for metadata and data files of an Iceberg table</li> 
 <li>A database for the Iceberg table in <a href="https://aws.amazon.com/glue" rel="noopener" target="_blank">AWS Glue</a> <a href="https://docs.aws.amazon.com/glue/latest/dg/start-data-catalog.html" rel="noopener" target="_blank">Data Catalog</a></li> 
 <li>An <a href="https://aws.amazon.com/iam/" rel="noopener" target="_blank">AWS Identity and Access Management (IAM)</a> role for an AWS Glue job</li> 
</ul> 
<p>Complete the following steps to deploy the resources.</p> 
<ol> 
 <li>Choose Launch stack</li> 
</ol> 
<p><a href="https://console.aws.amazon.com/cloudformation/home?#/stacks/new?templateURL=https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-4710/History.yaml&amp;stackName=history" rel="noopener" target="_blank"><img alt="Launch Button" class="alignnone wp-image-3705 size-full" height="20" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2017/11/15/LaunchStack.png" width="107" /></a></p> 
<ol start="2"> 
 <li>For the parameters,&nbsp;<strong>IcebergDatabaseName</strong> is set by default. You can change the default value. Then, choose <strong>Next</strong>.</li> 
 <li>Choose <strong>Next</strong></li> 
 <li>Choose&nbsp;<strong>I acknowledge that AWS CloudFormation might create IAM resources with custom names</strong>.</li> 
 <li>Choose&nbsp;<strong>Submit</strong>.</li> 
 <li>After the stack creation is complete, check the <strong>Outputs</strong> tab and make a note of the resource values, which are used in the following sections.</li> 
</ol> 
<p>Next, configure the Iceberg JAR files to the session to use the Iceberg change log view feature. Complete the following steps.</p> 
<ol> 
 <li>Select the following JAR files from the <a href="https://iceberg.apache.org/releases/" rel="noopener" target="_blank">Iceberg releases page</a> and download these JAR files on your local machine: 
  <ol> 
   <li><strong>1.6.1 Spark 3.3_with Scala 2.12 runtime Jar</strong>.</li> 
   <li><strong>1.6.1 aws-bundle Jar</strong>.</li> 
  </ol> </li> 
 <li>Open the <a href="https://console.aws.amazon.com/s3/buckets" rel="noopener" target="_blank">Amazon S3 console</a> and select the S3 bucket you created using the CloudFormation stack. The S3 bucket name can be found on the CloudFormation <strong>Outputs</strong> tab.<strong><br /> </strong></li> 
 <li>Choose <strong>Create folder</strong> and create the <code>jars</code> path in the S3 bucket.</li> 
 <li>Upload the two downloaded JAR files on <code>s3://&lt;IcebergS3Bucket&gt;/jars/</code> from the S3 console.</li> 
</ol> 
<h2>Upload a Jupyter Notebook on AWS Glue Studio</h2> 
<p>After launching the CloudFormation stack, create an AWS Glue Studio notebook to use Iceberg with AWS Glue.</p> 
<ol> 
 <li>Download <a href="https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-4710/history.ipynb" rel="noopener" target="_blank">history.ipynb</a>.</li> 
 <li>Open <a href="https://console.aws.amazon.com/gluestudio/home#/jobs" rel="noopener" target="_blank">AWS Glue Studio console</a>.</li> 
 <li>Under <strong>Create job</strong>, select <strong>Notebook</strong>.</li> 
 <li>Select <strong>Upload Notebook</strong>, choose <strong>Choose file</strong> and upload the Notebook you downloaded.</li> 
 <li>Select the IAM role name such as <strong>IcebergHistoryGlueJobRole</strong> that you created using the CloudFormation template. Then, choose <strong>Create notebook</strong>.</li> 
</ol> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/1_upload-notebook.png" rel="noopener" target="_blank"><img alt="1_upload-notebook" class="alignnone size-full wp-image-71330" height="1026" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/1_upload-notebook.png" style="margin: 10px 0px 10px 0px;" width="1638" /></a></p> 
<ol start="6"> 
 <li>For <strong>Job name</strong> at the left top of the page, enter <code>iceberg_history</code>.</li> 
 <li>Choose <strong>Save</strong>.</li> 
</ol> 
<h2>Create an Iceberg table</h2> 
<p>To create an Iceberg table using a product dataset, complete the following steps.</p> 
<ol> 
 <li>On the Jupyter Notebook that you created in <strong>Upload a Jupyter Notebook on AWS Glue Studio</strong>, run the following cell to use Iceberg with AWS Glue. Before running the cell, replace <code>&lt;IcebergS3Bucket&gt;</code> with the S3 bucket name where you uploaded the Iceberg JAR files.</li> 
</ol> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/2_session-config.png" rel="noopener" target="_blank"><img alt="2_session-config" class="alignnone size-full wp-image-71331" height="302" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/2_session-config.png" style="margin: 10px 0px 10px 0px;" width="2210" /></a></p> 
<ol start="2"> 
 <li>Initialize the SparkSession with Iceberg settings.</li> 
</ol> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/3_ss-init.png" rel="noopener" target="_blank"><img alt="3_ss-init" class="alignnone size-full wp-image-71332" height="392" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/3_ss-init.png" style="margin: 10px 0px 10px 0px;" width="1750" /></a></p> 
<ol start="3"> 
 <li>Configure database and table names for an Iceberg table (<code>DB_TBL</code>) and data warehouse path (<code>ICEBERG_LOC</code>). Replace&nbsp;<code>&lt;IcebergS3Bucket&gt;</code> with the S3 bucket from the CloudFormation <strong>Outputs</strong> tab.</li> 
 <li>Run the following code to create the Iceberg table using the Spark DataFrame based on the product dataset.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-python">from pyspark.sql import Row
import time
ut = time.time()
product = [
&nbsp;&nbsp; &nbsp;{'product_id': '00001', 'product_name': 'Heater', 'price': 250, 'category': 'Electronics', 'updated_at': ut},
&nbsp;&nbsp; &nbsp;{'product_id': '00002', 'product_name': 'Thermostat', 'price': 400, 'category': 'Electronics', 'updated_at': ut},
&nbsp;&nbsp; &nbsp;{'product_id': '00003', 'product_name': 'Television', 'price': 600, 'category': 'Electronics', 'updated_at': ut},
&nbsp;&nbsp; &nbsp;{'product_id': '00004', 'product_name': 'Blender', 'price': 100, 'category': 'Electronics', 'updated_at': ut},
&nbsp;&nbsp; &nbsp;{'product_id': '00005', 'product_name': 'USB charger', 'price': 50, 'category': 'Electronics', 'updated_at': ut}
]
df_products = spark.createDataFrame(Row(**x) for x in product)
df_products.createOrReplaceTempView('tmp')

spark.sql(f"""
CREATE TABLE {DB_TBL} USING iceberg LOCATION '{ICEBERG_LOC}'
AS SELECT * FROM tmp
""")</code></pre> 
</div> 
<ol start="5"> 
 <li>After creating the Iceberg table, run <code>SELECT * FROM iceberg_history_db.products ORDER BY product_id</code> to show the product data in the Iceberg table. Currently the following five products are stored in the Iceberg table.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">+----------+------------+-----+-----------+--------------------+
|product_id|product_name|price| &nbsp; category| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;updated_at|
+----------+------------+-----+-----------+--------------------+
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;250|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00002| &nbsp;Thermostat| &nbsp;400|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00003| &nbsp;Television| &nbsp;600|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00004| &nbsp; &nbsp; Blender| &nbsp;100|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00005| USB charger| &nbsp; 50|Electronics|1.7297845122056053E9|
+----------+------------+-----+-----------+--------------------+</code></pre> 
</div> 
<p>Next, look up the historical changes for a product using Iceberg’s change log view feature.</p> 
<h2>Implement historical record lookup with Iceberg’s change log view</h2> 
<p>Suppose that there’s a source table whose table records are replicated to the Iceberg table through a Change Data Capture (CDC) process. When the records in the source table are updated, these changes are then mirrored in the Iceberg table. In this section, you look up the history of a given record for such a system to capture the history of product updates. For example, the following updates occur in the source table. Through the CDC process, these changes are applied to the Iceberg table.</p> 
<ul> 
 <li>Upsert (update and insert) the two records: 
  <ul> 
   <li>The price of <code>Heater</code> (<code>product_id: 00001</code>) is updated from <code>250</code> to <code>500</code>.</li> 
   <li>A new product <code>Chair</code> (<code>product_id: 00006</code>) is added.</li> 
  </ul> </li> 
 <li><code>Television</code> (<code>product_id: 00003</code>) is deleted.</li> 
</ul> 
<p>To simulate the CDC workflow, you manually apply these changes to the Iceberg table in the notebook.</p> 
<ol> 
 <li>Use the <code>MERGE INTO</code> query to upsert records. If an input record in the Spark DataFrame has the same <code>product_id</code> as an existing record, the existing record is updated. If no matching <code>product_id</code> is found, the input record is inserted into the Iceberg table.</li> 
</ol> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/4-merge-into.png" rel="noopener" target="_blank"><img alt="4-merge-into" class="alignnone size-full wp-image-71333" height="640" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/4-merge-into.png" style="margin: 10px 0px 10px 0px;" width="2248" /></a></p> 
<ol start="2"> 
 <li>Delete <code>Television</code> from the Iceberg table by running the <code>DELETE</code> query.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-sql">DELETE FROM iceberg_history_db.products WHERE product_id = '00003'</code></pre> 
</div> 
<ol start="3"> 
 <li>Then, run <code>SELECT * FROM iceberg_history_db.products ORDER BY product_id</code> to show the product data in the Iceberg table. You can confirm that the price of <code>Heater</code> is updated to <code>500</code>, Chair is added and <code>Television</code> is deleted.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">+----------+------------+-----+-----------+--------------------+
|product_id|product_name|price| &nbsp; category| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;updated_at|
+----------+------------+-----+-----------+--------------------+
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater|&nbsp; 500|Electronics| &nbsp; &nbsp;1.729790106579E9|
| &nbsp; &nbsp; 00002| &nbsp;Thermostat|&nbsp; 400|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00004| &nbsp; &nbsp; Blender| &nbsp;100|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00005| USB charger| &nbsp; 50|Electronics|1.7297845122056053E9|
| &nbsp; &nbsp; 00006| &nbsp; &nbsp; &nbsp; Chair| &nbsp; 50| &nbsp;Furniture| &nbsp; &nbsp;1.729790106579E9|
+----------+------------+-----+-----------+--------------------+</code></pre> 
</div> 
<p>For the Iceberg table, where changes from the source table are replicated, you can track the record changes using Iceberg’s change log view. To start, you first create a change log view from the Iceberg table.</p> 
<ol start="4"> 
 <li>Run the <code>create_changelog_view</code> Iceberg procedure to create a change log view.</li> 
</ol> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/6-clv.png" rel="noopener" target="_blank"><img alt="5-clv" class="alignnone size-full wp-image-71334" height="268" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/6-clv.png" style="margin: 10px 0px 10px 0px;" width="1020" /></a></p> 
<ol start="5"> 
 <li>Run the following query to retrieve the historical changes for <code>Heater</code>.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-sql">SELECT product_id, product_name, price, category, updated_at, _change_type
FROM products_clv WHERE product_id = '00001'
ORDER BY _change_ordinal, _change_type DESC</code></pre> 
</div> 
<ol start="6"> 
 <li>The query result shows the historical changes to <code>Heater</code>. You can confirm that the price of <code>Heater</code> was updated from <code>250</code> to <code>500</code> from the output.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">+----------+------------+-----+-----------+--------------------+-------------+
|product_id|product_name|price| &nbsp; category| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;updated_at| _change_type|
+----------+------------+-----+-----------+--------------------+-------------+
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;250|Electronics|1.7297902833360643E9| &nbsp; &nbsp; &nbsp; INSERT|
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;250|Electronics|1.7297902833360643E9|UPDATE_BEFORE|
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;500|Electronics|1.7297903836233025E9| UPDATE_AFTER|
+----------+------------+-----+-----------+--------------------+-------------+</code></pre> 
</div> 
<p>Using Iceberg’s change log view, you can obtain the history of a given record directly from the Iceberg table’s history, without needing to create a separate table for managing record history. Next, you implement Slowly Changing Dimension (SCD) Type-2 using the change log view.</p> 
<h2>Implement SCD Type-2 with Iceberg’s change log view</h2> 
<p>The SCD Type-2 based table retains the full history of record changes and it can be used in multiple cases such as historical tracking, point-in-time analysis, regulatory compliance, and so on. In this section, you implement SCD Type-2 using the change log view (<code>products_clv</code>) that was created in the previous section. The change log view has a schema that’s similar to the schema defined in the SCD Type-2 specifications. For this change log view, you add&nbsp;<code>effective_start</code>, <code>effective_end</code>, and <code>is_current</code> columns. To add these columns and then implement SCD Type-2, complete the following steps.</p> 
<ol> 
 <li>Run the following query to implement SCD Type-2. In the <code>WITH AS (...)</code> section of the query, the change log view is merged with the Iceberg table snapshots using the <code>snapshot_id</code> key to include the commit time for each record change. You can obtain the table snapshots by querying for <code>db.table.snapshots</code>. The other part in the query identifies both current and non-current entries by comparing the commit times for each product. It then sets the effective time for each product, and marks whether a product is current or not based on the effective time and the change type from the change log view.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-sql">WITH clv_snapshots AS (
    SELECT
        clv.*,
        s.snapshot_id,
        s.committed_at,
        s.committed_at as effective_start
    FROM products_clv clv
    JOIN iceberg_history_db.products.snapshots s
    ON clv._commit_snapshot_id = s.snapshot_id
) 
SELECT
    product_id, 
    product_name, 
    price, 
    category, 
    updated_at,
    effective_start,
    CASE
        WHEN effective_start != l_part_committed_at 
            OR _change_type = 'UPDATE_BEFORE' THEN l_part_committed_at
        ELSE CAST(null as timestamp)
    END as effective_end,
    CASE
        WHEN effective_start != l_part_committed_at
            OR _change_type = 'UPDATE_BEFORE' 
            OR _change_type = 'DELETE' THEN CAST(false as boolean)
        ELSE CAST(true as boolean)
    END as is_current
FROM (SELECT *, MAX(committed_at) OVER (PARTITION BY product_id, updated_at) as l_part_committed_at FROM clv_snapshots)
WHERE _change_type != 'UPDATE_BEFORE'
ORDER BY product_id,  _change_ordinal</code></pre> 
</div> 
<ol start="2"> 
 <li>The query result shows the SCD Type-2 based schema and records.</li> 
</ol> 
<p><a href="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/8-output.png" rel="noopener" target="_blank"><img alt="7-output" class="alignnone size-full wp-image-71336" height="470" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/05/8-output.png" style="margin: 10px 0px 10px 0px;" width="2074" /></a></p> 
<p>After the query result is displayed, this SCD Type-2 based table is stored as <code>scdt2</code>&nbsp;to allow access for further analysis.</p> 
<p>SCD Type-2 is useful for many use cases. To explore how this SCD Type-2 implementation can be used to track the history of table records, run the following example queries.</p> 
<ol start="3"> 
 <li>Run the following query to retrieve deleted or updated records in a specific period. This query captures which records were changed during that timeframe, allowing you to audit changes for further use-cases such as trend analysis, regulatory compliance checks, and so on. Before running the query, replace <code>&lt;START_DATETIME&gt;</code> and <code>&lt;END_DATETIME&gt;</code> with specific time ranges such as <code>2024-10-24 17:18:00</code> and <code>2024-10-24 17:20:00</code>.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-sql">SELECT product_id, product_name, price, category, updated_at, effective_start, effective_end, is_current 
FROM scdt2 WHERE product_id IN ( SELECT product_id FROM scdt2 
WHERE (_change_type = 'DELETE' or _change_type = 'UPDATE_AFTER') 
AND effective_start BETWEEN '&lt;START_DATETIME&gt;' AND '&lt;END_DATETIME&gt;') 
ORDER BY product_id, effective_start
</code></pre> 
</div> 
<ol start="4"> 
 <li>The query result shows the deleted and updated records in the specified period. You can confirm that the price of <code>Heater</code> was updated and <code>Television</code> was deleted from the table.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">+----------+------------+-----+-----------+--------------------+--------------------+--------------------+----------+
|product_id|product_name|price| &nbsp; category| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;updated_at| &nbsp; &nbsp; effective_start| &nbsp; &nbsp; &nbsp; effective_end|is_current|
+----------+------------+-----+-----------+--------------------+--------------------+--------------------+----------+
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;250|Electronics|1.7297902833360643E9|2024-10-24 17:18:...|2024-10-24 17:19:...| &nbsp; &nbsp; false|
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;500|Electronics|1.7297903836233025E9|2024-10-24 17:19:...| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;null| &nbsp; &nbsp; &nbsp;true|
| &nbsp; &nbsp; 00003| &nbsp;Television| &nbsp;600|Electronics|1.7297902833360643E9|2024-10-24 17:18:...|2024-10-24 17:19:...| &nbsp; &nbsp; false|
| &nbsp; &nbsp; 00003| &nbsp;Television| &nbsp;600|Electronics|1.7297902833360643E9|2024-10-24 17:19:...| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;null| &nbsp; &nbsp; false|
+----------+------------+-----+-----------+--------------------+--------------------+--------------------+----------+</code></pre> 
</div> 
<ol start="5"> 
 <li>As another example, run the following query to retrieve the latest records at a specific point in time from the SCD Type-2 table by filtering with <code>is_current = true</code> for current data reporting.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-sql">SELECT product_id, product_name, price, category, updated_at
FROM scdt2 WHERE is_current = true ORDER BY product_id</code></pre> 
</div> 
<ol start="6"> 
 <li>The query result shows the current table records, reflecting the updated price of <code>Heater</code>, the deletion of <code>Television</code>, and the addition of <code>Chair</code> after the initial records.</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-txt">+----------+------------+-----+-----------+--------------------+
|product_id|product_name|price| &nbsp; category| &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;updated_at|
+----------+------------+-----+-----------+--------------------+
| &nbsp; &nbsp; 00001| &nbsp; &nbsp; &nbsp;Heater| &nbsp;500|Electronics|1.7297903836233025E9|
| &nbsp; &nbsp; 00002| &nbsp;Thermostat| &nbsp;400|Electronics|1.7297902833360643E9|
| &nbsp; &nbsp; 00004| &nbsp; &nbsp; Blender| &nbsp;100|Electronics|1.7297902833360643E9|
| &nbsp; &nbsp; 00005| USB charger| &nbsp; 50|Electronics|1.7297902833360643E9|
| &nbsp; &nbsp; 00006| &nbsp; &nbsp; &nbsp; Chair| &nbsp; 50| &nbsp;Furniture|1.7297903836233025E9|
+----------+------------+-----+-----------+--------------------+</code></pre> 
</div> 
<p>You have now successfully implemented SCD Type-2 using the change log view. This SCD Type-2 implementation allows you to track the history of table records. For example, you can use it to search for deleted or updated products such as <code>Heater</code> and <code>Chair</code> in a specific period. Additionally, you can&nbsp;retrieve the current table records by querying the SCD Type-2 table with <code>is_current = true</code>. Using Iceberg’s change log view enables you to implement SCD Type-2 without making any changes to the Iceberg table itself. It also eliminates the need for creating or managing an additional table for SCD Type-2.</p> 
<h2>Clean up</h2> 
<p>To clean up the resources used in this post, complete the following steps:</p> 
<ol> 
 <li>Open the <a href="https://console.aws.amazon.com/s3/buckets" rel="noopener" target="_blank">Amazon S3 console</a></li> 
 <li>Select the S3 bucket <code>aws-glue-assets-&lt;ACCOUNT_ID&gt;-&lt;REGION&gt;</code> where the Notebook file (<code>iceberg_history.ipynb</code>) is stored. Delete the Notebook file that’s in the notebook path.</li> 
 <li>Select the S3 bucket you created using the CloudFormation template. You can obtain the bucket name from&nbsp;<strong>IcebergS3Bucket</strong> key on the CloudFormation&nbsp;<strong>Outputs</strong> tab. After selecting the bucket, choose <strong>Empty</strong> to delete all objects</li> 
 <li>After you confirm the bucket is empty, delete the CloudFormation stack <code>iceberg-history-baseline-resources</code>.</li> 
</ol> 
<h2>Considerations</h2> 
<p>Here are important considerations:</p> 
<ul> 
 <li>The change log view does not lose any historical record changes even when following operations are performed: 
  <ul> 
   <li>Compaction: <a href="https://iceberg.apache.org/docs/1.6.1/spark-procedures/#rewrite_data_files" rel="noopener" target="_blank"><code>rewrite_data_files</code></a> or <a href="https://docs.aws.amazon.com/glue/latest/dg/compaction-management.html" rel="noopener" target="_blank">Glue Data Catalog automatic compaction</a>.</li> 
   <li>Orphan file deletion: <a href="https://iceberg.apache.org/docs/1.6.1/spark-procedures/#remove_orphan_files" rel="noopener" target="_blank"><code>remove_orphan_files</code></a> or <a href="https://docs.aws.amazon.com/glue/latest/dg/orphan-file-deletion.html" rel="noopener" target="_blank">Glue Data Catalog automatic orphan file deletion</a>.</li> 
  </ul> </li> 
 <li>The change log view loses historical record changes corresponded to snapshots deleted with <a href="https://iceberg.apache.org/docs/1.6.1/spark-procedures/#expire_snapshots" rel="noopener" target="_blank"><code>expire_snapshots</code></a> and <a href="https://docs.aws.amazon.com/glue/latest/dg/snapshot-retention-management.html" rel="noopener" target="_blank">Glue Data Catalog automatic snapshot deletion</a>.</li> 
 <li>The change log view is not supported in MoR tables.</li> 
</ul> 
<h2>Conclusion</h2> 
<p>In this post, we have explored how to look up the history of records and tables using Apache Iceberg. The instruction demonstrated how to use change log view to look up the history of the records, and also the history of the tables with SCD Type-2. With this method, you can manage the history of records and tables without extra effort.</p> 
<hr /> 
<h3>About the Authors</h3> 
<p style="clear: both;"><img alt="" class="size-full wp-image-30835 alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2022/06/13/tomtan.jpg" width="100" /><strong>Tomohiro Tanaka</strong> is a Senior Cloud Support Engineer at Amazon Web Services. He’s passionate about helping customers use Apache Iceberg for their data lakes on AWS. In his free time, he enjoys a coffee break with his colleagues and making coffee at home.</p> 
<p style="clear: both;"><img alt="" class="size-full wp-image-16628 alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2021/02/10/Noritaka-Sekiyama-p.png" width="100" /><strong>Noritaka Sekiyama</strong> is a Principal Big Data Architect on the AWS Glue team. He works based in Tokyo, Japan. He is responsible for building software artifacts to help customers. In his spare time, he enjoys cycling with his road bike.</p>
