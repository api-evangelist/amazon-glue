---
title: "Secure generative SQL with Amazon Q"
url: "https://aws.amazon.com/blogs/big-data/secure-generative-sql-with-amazon-q/"
date: "Fri, 25 Jul 2025 16:25:20 +0000"
author: "Gregory Knowles"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p><a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai.html" rel="noopener noreferrer" target="_blank">Amazon Q generative SQL</a> brings generative AI capabilities to help speed up deriving insights from your <a href="https://aws.amazon.com/redshift/" rel="noopener noreferrer" target="_blank">Amazon Redshift </a>data warehouses and <a href="https://aws.amazon.com/glue/" rel="noopener noreferrer" target="_blank">AWS Glue</a> Data Catalog, generating SQL for Amazon Redshift or <a href="https://aws.amazon.com/athena/" rel="noopener noreferrer" target="_blank">Amazon Athena</a>. With <a href="https://aws.amazon.com/q" rel="noopener noreferrer" target="_blank">Amazon Q</a>, you get SQL commands generated with your context. This means you can focus on deriving insights faster, rather than having to first learn potentially complex schemas. Without generative SQL, your data analysts might have to frequently switch between different types of SQL, which can further slow analysis down. Amazon Q generative SQL can help by generating SQL statements from natural language and speeding up development. This can help onboard analysts faster and improve analyst productivity. The generative SQL experience is available through <a href="https://aws.amazon.com/sagemaker/unified-studio/" rel="noopener noreferrer" target="_blank">Amazon SageMaker Unified Studio</a> and <a href="https://aws.amazon.com/redshift/query-editor-v2/" rel="noopener noreferrer" target="_blank">Amazon Redshift Query Editor v2</a>.</p> 
<p>To scale the use of generative SQL in production scenarios, you need to consider how relevant and accurate SQL is generated. In doing so, it’s important to understand what data is used and how your information is protected. Amazon Q generative SQL is designed to keep your data secure and private. Your queries, data, and database schemas are not used to train generative AI foundation models (FMs). For more information, see <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai.html#query-editor-v2-generative-ai-considerations" rel="noopener noreferrer" target="_blank">Considerations when interacting with Amazon Q generative SQL</a>.</p> 
<p>In the post <a href="https://aws.amazon.com/blogs/big-data/write-queries-faster-with-amazon-q-generative-sql-for-amazon-redshift/" rel="noopener noreferrer" target="_blank">Write queries faster with Amazon Q generative SQL for Amazon Redshift</a>, we provided general advice around getting started with generative SQL. In this post, we discuss the design and security controls in place when using generative SQL and its use in both SageMaker Unified Studio and Amazon Redshift Query Editor v2.</p> 
<h2>Solution overview</h2> 
<p>Generating relevant SQL requires context from your data warehouse or data catalog schemas. Your analysts can ask free text or natural language questions in the Amazon Q chat window and have SQL statements returned that reference your tables and columns. It’s important that the generated SQL is consistent with your schema so that it can find the most relevant fields to answer questions and generate queries that accurately reference data. In SageMaker Unified Studio or Amazon Redshift Query Editor v2, when the Amazon Q chat window is open, database metadata that is viewable under the connection context is made available to Amazon Q for SQL generation. This means that only the schema information that the connecting user can access is used. Tables or database objects the user doesn’t have access to are excluded.</p> 
<p>When a user submits questions in the Amazon Q chat window, a search algorithm is used to find the most relevant context from the available database schema metadata information. This context is combined with the user’s question and used as a prompt to a large language model (LLM) to generate a SQL statement. The supporting information is cached so that your data source doesn’t need to be queried every time a user initiates SQL generation. Instead, data source metadata will be periodically refreshed if it remains in use, or you can <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai-interact.html" rel="noopener noreferrer" target="_blank">trigger a manual refresh</a>. If the data is not being used, Amazon Q will automatically delete it. Where applicable, the information used to support SQL generation is encrypted with an <a href="https://aws.amazon.com/kms/" rel="noopener noreferrer" target="_blank">AWS Key Management Service</a> (AWS KMS) customer managed KMS key where one has been specified in the SageMaker Unified Studio or Amazon Redshift Query Editor v2 settings. Otherwise, an AWS managed key is used. Your information is encrypted in transit and at rest.</p> 
<p>The following diagram shows the process flow for SQL generation when using SageMaker Unified Studio or the Amazon Redshift Query Editor and using Amazon Redshift or Data Catalog source data.</p> 
<p><img alt="Process diagram for SQL generation" class="alignnone wp-image-80528 size-large" height="586" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/07/09/generative-sql-arch-1024x586.jpeg" width="1024" /></p> 
<p>The Amazon Q generative SQL process can be summarized as the following steps:</p> 
<ol> 
 <li>A user interacts with the Amazon Q chat pane through SageMaker Unified Studio or the Amazon Redshift Query Editor.</li> 
 <li>The SQL chat frontend sends the prompt along with the connection configuration to Amazon Q.</li> 
 <li>Amazon Q uses the connection context to retrieve information that will support SQL generation if this data is not already available.</li> 
 <li>Amazon Q encrypts the retrieved information under the appropriate AWS managed or customer managed KMS key. The information is subsequently decrypted on retrieval.</li> 
 <li>The information is stored along with custom context information, if this has been provided.</li> 
 <li>Relevant context from the combined information is selected and added to the user’s questions and sent to an LLM to generate a SQL statement, which is returned to the user.</li> 
 <li>The user can decide whether to run the statement and can provide feedback on usefulness and accuracy.</li> 
</ol> 
<h3>Additional context to enhance SQL generation</h3> 
<p>You can provide further context to supplement the database schema information, which can help improve the accuracy and relevancy of the generated SQL.</p> 
<p>One option is to provide <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai.html#query-editor-v2-generative-custom-context" rel="noopener noreferrer" target="_blank">custom context</a>. Custom context gives the option to specify instructions and extra information, such as descriptions of tables and columns. These descriptions can then be used to help the selection of relevant tables and attributes when generating SQL statements. This is particularly relevant when your schema uses more obscure naming that might not directly relate to business terms or uses non-standard abbreviations. For example, consider a table called <code>sls_r1_2024</code><em>. </em>With custom context, you can add a table description specifying that, for example, the table includes sales information across stores in the US region for the calendar year 2024. This information can help the LLM generate SQL referencing the correct tables. The same approach can be applied to columns within the table. Your custom context is encrypted using a customer managed KMS key if one has been specified (during Amazon Redshift Query Editor account creation or SageMaker Unified Studio project creation) or an AWS managed key otherwise.</p> 
<p>You can also introduce constraints using custom context. For example, you can explicitly include or exclude specific schemas, tables, or columns from SQL query generation. Similarly, specific topics can also be disallowed, such as not generating SQL statements to support financial reporting. For more details about the information that can be supplied, refer to <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai.html#query-editor-v2-generative-custom-context" rel="noopener noreferrer" target="_blank">Custom context</a>.</p> 
<p>Another option is to grant SQL query history access to the user establishing the connection. This information is then also made available to enhance SQL generation and to provide the LLM with examples of relevant queries. Be aware that granting wider SQL query history access to the connecting user, and therefore also the generative SQL workflow, allows viewing of queries over tables or objects the user might not have access to. Furthermore, string literals might be present in historic statements that might contain sensitive information. To help mitigate this risk, you could instead use the <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai.html#query-editor-v2-generative-custom-context" rel="noopener noreferrer" target="_blank">CuratedQueries</a> section of custom context to provide predefined question and answer examples, without exposing all user queries.</p> 
<h3>Generated statement response</h3> 
<p>Before a SQL statement is returned to the user, Amazon Q tries to detect syntax issues. This step helps improve the likelihood that only valid SQL syntax is returned. Amazon Q will use the available information for the user to return statements that align with user permissions, to reduce scenarios where users can’t run generated statements. For example, if you have given access to SQL query history information, then the SQL generation step might produce a query statement referencing a table that the user asking the question doesn’t have access to. Amazon Q minimizes the occurrence of this scenario by assessing if the generated SQL aligns with user permissions and updating the statement if not. User permissions are not bypassed through the use of Amazon Q generative SQL. If a statement was returned referencing a table the user doesn’t have access to, the authorization applied to the user will enforce access control when the statement is executed.</p> 
<p>Statements generated by Amazon Q that could potentially change your database, such as DML or DDL statements, are returned with a warning. The warning highlights to the user that running the statement could potentially modify the database. Again, these statements are only executable if the user has the required permissions.</p> 
<h2>Prerequisites</h2> 
<p>Amazon Q generative SQL works with your Redshift data warehouses and Data Catalog tables. To get started, you should have data available in either or both of these environments. To use Amazon Q generative SQL with your AWS Glue tables, you need a SageMaker Unified Studio domain. Within your domain, you can use the Amazon Q chat integration to ask questions of your data and have SQL generated. This also works for Amazon Redshift data sources available in the domain. You can use Amazon Q generative SQL without a SageMaker Unified Studio domain using the Amazon Redshift Query Editor. Access to the editor enables Amazon Q chat integration against your Amazon Redshift data sources.</p> 
<h2>Enable Amazon Q generative SQL</h2> 
<p>You can control access to generative SQL at the account-Region level in the Amazon Redshift Query Editor or at the SageMaker Unified Studio domain level. To enable this feature, an account admin must explicitly turn on Amazon Q generative SQL. By default, the feature is not accessible to your users. Administrators that have permission for the <code>sqlworkbench:UpdateAccountQSqlSettings</code> <a href="https://aws.amazon.com/iam/" rel="noopener noreferrer" target="_blank">AWS Identity and Access Management</a> (IAM) action can turn the Amazon Q generation SQL feature on or off through the admin window, as illustrated in the following sections. When turned off, this will restrict users from opening the Amazon Q chat pane and help prevent interaction with generative SQL.</p> 
<h3>Enable Amazon Q in your SageMaker domain</h3> 
<p>To enable Amazon Q in your SageMaker domain, you can navigate to the <strong>Amazon Q</strong> tab on the domain settings page and choose to enable the service. For more information, see <a href="https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/amazonq.html" rel="noopener noreferrer" target="_blank">Amazon Q in Amazon SageMaker Unified Studio</a>.</p> 
<p><img alt="Enable Amazon Q in SageMaker Unified Studio domain" class="alignnone wp-image-80530 size-large" height="443" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/07/09/sagemaker-domain-1024x443.jpeg" width="1024" /></p> 
<h3>Enable Amazon Q in Amazon Redshift</h3> 
<p>To enable Amazon Q generative SQL from the Amazon Redshift Query Editor, access the Amazon Q generative SQL settings. This requires the administrator to have the <code>sqlworkbench:UpdateAccountQSqlSettings</code> permission in their IAM policy. For more information, see <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai-settings.html" rel="noopener noreferrer" target="_blank">Updating generative SQL settings as an administrator</a>.</p> 
<p><img alt="Enabling Amazon Q generative SQL from Redshift query editor" class="alignnone wp-image-80529 size-large" height="709" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/07/09/q-settings-1024x709.jpeg" width="1024" /></p> 
<p>With generative SQL enabled at the account-Region level, you can restrict access to specific users with IAM controls. IAM administrators can build IAM policies that allow or deny access to the action <code>sqlworkbench:GetQSqlRecommendations</code><em>. </em>For more information, refer to <a href="https://docs.aws.amazon.com/service-authorization/latest/reference/list_awssqlworkbench.html" rel="noopener noreferrer" target="_blank">Actions, resources, and condition keys for AWS SQL Workbench</a>. Policies can then be associated with IAM users or roles to control access to SQL generation at a more granular level. An appropriately scoped <a href="https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html" rel="noopener noreferrer" target="_blank">service control policy</a> (SCP) can be used to limit access to SQL generation to specific accounts within your organization if required.</p> 
<p>The following is an example policy denying access to use SQL generation:</p> 
<div class="hide-language"> 
 <pre><code class="lang-css">{
"Version": "2012-10-17",
&nbsp;&nbsp; &nbsp;"Statement": [
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;{
"Sid": "DenyAccessToAmazonQGenerativeSql",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"Effect": "Deny",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"Action": [
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"sqlworkbench:GetQSqlRecommendations"
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;],
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"Resource": "*",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;}
&nbsp;&nbsp; &nbsp;]
}</code></pre> 
</div> 
<h2>Cross-Region inference</h2> 
<p>Amazon Q Developer uses cross-Region inference to distribute traffic across different AWS Regions, which provides increased throughput and resilience during high demand periods, improved performance, and access to the latest Amazon Q Developer capabilities.</p> 
<p>When a request is made from an Amazon Q Developer profile, it is kept within the Regions in the same geography as the original data. Although this doesn’t change where the data is stored, the requests and output results might move across Regions during the inference process. Data is encrypted when transmitted across Amazon’s network. For more information on cross-Region inference, see <a href="https://docs.aws.amazon.com/amazonq/latest/qdeveloper-ug/cross-region-processing.html" rel="noopener noreferrer" target="_blank">Cross-region processing in Amazon Q Developer</a>.</p> 
<h2>Monitoring</h2> 
<p>To monitor which IAM users or roles are interacting with generative SQL, you can use <a href="https://aws.amazon.com/cloudtrail/" rel="noopener noreferrer" target="_blank">AWS CloudTrail</a>. CloudTrail monitors API calls and logs which identities have performed particular actions. When a user first asks a question, a CloudTrail event is emitted called <code>IngestQSqlMetadata</code><em>. </em>This is a result of Amazon Q starting the metadata ingest process. Ingestion is an asynchronous operation, so there might be a series of <code>GetQSqlMetadataStatus</code> events. This is due to the workflow checking the ingestion process status.</p> 
<p>After the workflow has completed successfully, each question sees a <code>GetQSqlRecommendation</code> event. This is the result of users submitting questions and triggering generation of SQL statements. The following is an example CloudTrail event for <code>GetQSqlRecommendation</code>. In this example, Amazon Q emits detailed CloudTrail events highlighting the warehouse being queried, IAM principal calling Amazon Q, and the entire response structure from Amazon Q in <code>responseElements</code>:</p> 
<div class="hide-language"> 
 <pre><code class="lang-css">{
&nbsp;&nbsp; &nbsp;"eventVersion": "1.09",
&nbsp;&nbsp; &nbsp;"userIdentity": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"type": "AssumedRole",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"principalId": "AROA123456789EXAMPLE:demouser",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"arn": "arn:aws:sts::111122223333:assumed-role/DemoUser",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"accountId": "111122223333",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"accessKeyId": "ASIAIOSFODNN7EXAMPLE",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"sessionContext": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"sessionIssuer": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"type": "Role",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"principalId": "AROA123456789EXAMPLE",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"arn": "arn:aws:iam::111122223333:role/DemoUser",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"accountId": "111122223333",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"userName": "DemoUser"
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"attributes": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"creationDate": "2025-01-17T05:31:01Z",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"mfaAuthenticated": "false"
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;}
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;}
&nbsp;&nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp;"eventTime": "2025-01-17T05:34:51Z",
&nbsp;&nbsp; &nbsp;"eventSource": "sqlworkbench.amazonaws.com",
&nbsp;&nbsp; &nbsp;"eventName": "GetQSqlRecommendation",
&nbsp;&nbsp; &nbsp;"awsRegion": "us-east-1",
&nbsp;&nbsp; &nbsp;"sourceIPAddress": "122.171.17.139",
&nbsp;&nbsp; &nbsp;"userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
&nbsp;&nbsp; &nbsp;"requestParameters": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"dbConfig": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"database": "sample_data_dev"
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"databaseConfiguration": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"redshiftConfig": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"clusterIdentifier": "redshift-cluster-1",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"database": "sample_data_dev"
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;}
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"prompt": "HIDDEN_DUE_TO_SECURITY_REASONS",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"clientToken": "HIDDEN_DUE_TO_SECURITY_REASONS",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"logConfig": {},
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"sqlworkbenchConnectionArn": "arn:aws:sqlworkbench:us-east-1:111122223333:connection/47ahg61-ce0b-4646-831b-a140ea4055ae"
&nbsp;&nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp;"responseElements": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"data": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"extractionErrors": false,
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"guardRails": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"isDml": false
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"sqlStatement": "HIDDEN_DUE_TO_SECURITY_REASONS",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"syntaxErrors": "HIDDEN_DUE_TO_SECURITY_REASONS"
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"logSessionId": "623318ad-dbcc-4f69-ae08-f85d1b63a70f",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"questionId": "623318ad-dbcc-4f69-ae08-f85d1b63a70f",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"originalQuestionId": "623318ad-dbcc-4f69-ae08-ae08asd1a"
&nbsp;&nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp;"requestID": "623318ad-dbcc-4f69-ae08-f85d1b63a70f",
&nbsp;&nbsp; &nbsp;"eventID": "ac2c1932-49b1-41b3-a1af-20fa4461cf7d",
&nbsp;&nbsp; &nbsp;"readOnly": false,
&nbsp;&nbsp; &nbsp;"eventType": "AwsApiCall",
&nbsp;&nbsp; &nbsp;"managementEvent": true,
&nbsp;&nbsp; &nbsp;"recipientAccountId": "111122223333",
&nbsp;&nbsp; &nbsp;"eventCategory": "Management",
&nbsp;&nbsp; &nbsp;"tlsDetails": {
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"tlsVersion": "TLSv1.3",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"cipherSuite": "TLS_AES_128_GCM_SHA256",
&nbsp;&nbsp; &nbsp; &nbsp; &nbsp;"clientProvidedHostHeader": "qsql.sqlworkbench.us-east-1.amazonaws.com"
&nbsp;&nbsp; &nbsp;},
&nbsp;&nbsp; &nbsp;"sessionCredentialFromConsole": "true"
}</code></pre> 
</div> 
<h2>Conclusion</h2> 
<p>In this post, we discussed the Amazon Q generative SQL workflow. We highlighted the process around using your schema context alongside metadata such as historic SQL queries and custom context. Using this metadata allows the generation of relevant SQL that helps accelerate your analyst’s productivity. Although it’s important to assist analysts, it’s also imperative to make sure data remains secure and protected. To support this, generative SQL uses only the data the connected user has access to. This helps prevent exposure to information beyond their authorization.When you’re looking to increase the relevance of generated SQL through sharing additional query history, it’s important to consider the trade-off of exposing additional information to the user. Deciding your approach here should take into account the domain context of the data and the possible exposure of metadata the user doesn’t have access to, or potentially sensitive information that might appear in query strings. Keeping these considerations in mind can help you achieve the appropriate security posture for your workloads.</p> 
<p>To get started with Amazon Q generative SQL, see <a href="https://aws.amazon.com/blogs/big-data/write-queries-faster-with-amazon-q-generative-sql-for-amazon-redshift/" rel="noopener noreferrer" target="_blank">Write queries faster with Amazon Q generative SQL for Amazon Redshift</a> and <a href="https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-generative-ai.html" rel="noopener noreferrer" target="_blank">Interacting with Amazon Q generative SQL</a>.</p> 
<hr /> 
<h3>About the authors</h3> 
<p style="clear: both;"><img alt="" class="alignleft wp-image-80534 size-thumbnail" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/07/09/greghk-100x133.jpg" width="100" /><strong>Gregory Knowles</strong> is a data and AI specialist solution architect at AWS, focusing on the UK public sector. With extensive experience in cloud-based architectures, Greg guides public sector customers in implementing modern data solutions. His expertise spans governance, analytics, and AI/ML. Greg’s passion lies in accelerating transformation and innovation to improve productivity and outcomes. He has successfully led projects that moved data systems into the cloud, adopted new data architectures, and implemented AI at scale in production.</p> 
<p style="clear: both;"><img alt="" class="alignleft wp-image-80533 size-thumbnail" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/07/09/abhitm-100x133.jpg" width="100" /><strong>Abhinav Tripathy</strong> is a Software Engineer and Security Guardian at AWS, where he develops Amazon Q generative SQL by combining machine learning, databases, and web systems. Abhinav is passionate about building scalable web systems from scratch that solve real customer challenges. Outside of work, he enjoys traveling, watching soccer, and playing badminton.</p> 
<p style="clear: both;"><img alt="" class="alignleft wp-image-80539 size-thumbnail" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/07/09/murtezao-100x133.jpg" width="100" /><strong>Erol Murtezaoglu </strong>is a Technical Product Manager at AWS, is an inquisitive and enthusiastic thinker with a drive for self-improvement and learning. He has a strong and proven technical background in software development and architecture, balanced with a drive to deliver commercially successful products. Erol highly values the process of understanding customer needs and problems, in order to deliver solutions that exceed expectations.</p>
