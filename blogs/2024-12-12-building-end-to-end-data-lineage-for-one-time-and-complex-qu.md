---
title: "Building end-to-end data lineage for one-time and complex queries using Amazon Athena, Amazon Redshift, Amazon Neptune and dbt"
url: "https://aws.amazon.com/blogs/big-data/building-end-to-end-data-lineage-for-one-time-and-complex-queries-using-amazon-athena-amazon-redshift-amazon-neptune-and-dbt/"
date: "Thu, 12 Dec 2024 17:17:33 +0000"
author: "Nancy Wu"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>One-time and complex queries are two common scenarios in enterprise data analytics. One-time queries are flexible and suitable for instant analysis and exploratory research. Complex queries, on the other hand, refer to large-scale data processing and in-depth analysis based on petabyte-level data warehouses in massive data scenarios. These complex queries typically involve data sources from multiple business systems, requiring multilevel nested SQL or associations with numerous tables for highly sophisticated analytical tasks.</p> 
<p>However, combining the data lineage of these two query types presents several challenges:</p> 
<ol> 
 <li>Diversity of data sources</li> 
 <li>Varying query complexity</li> 
 <li>Inconsistent granularity in lineage tracking</li> 
 <li>Different real-time requirements</li> 
 <li>Difficulties in cross-system integration</li> 
</ol> 
<p>Moreover, maintaining the accuracy and completeness of lineage information while providing system performance and scalability are crucial considerations. Addressing these challenges requires a carefully designed architecture and advanced technical solutions.</p> 
<p><a href="https://aws.amazon.com/athena/" rel="noopener" target="_blank">Amazon Athena</a> offers serverless, flexible SQL analytics for one-time queries, enabling direct querying of <a href="https://aws.amazon.com/s3/" rel="noopener" target="_blank">Amazon Simple Storage Service</a> (Amazon S3) data for rapid, cost-effective instant analysis. <a href="https://aws.amazon.com/redshift/" rel="noopener" target="_blank">Amazon Redshift</a>, optimized for complex queries, provides high-performance columnar storage and massively parallel processing (MPP) architecture, supporting large-scale data processing and advanced SQL capabilities. <a href="https://aws.amazon.com/neptune/" rel="noopener" target="_blank">Amazon Neptune</a>, as a graph database, is ideal for data lineage analysis, offering efficient relationship traversal and complex graph algorithms to handle large-scale, intricate data lineage relationships. The combination of these three services provides a powerful, comprehensive solution for end-to-end data lineage analysis.</p> 
<p>In the context of comprehensive data governance, <a href="https://aws.amazon.com/datazone/" rel="noopener" target="_blank">Amazon DataZone</a> offers organization-wide data lineage visualization using <a href="https://aws.amazon.com/" rel="noopener" target="_blank">Amazon Web Services</a> (AWS) services, while <a href="https://aws.amazon.com/marketplace/pp/prodview-tjpcf42nbnhko" rel="noopener" target="_blank">dbt</a> provides project-level lineage through model analysis and supports cross-project integration between data lakes and warehouses.</p> 
<p>In this post, we use dbt for data modeling on both Amazon Athena and Amazon Redshift. dbt on Athena supports real-time queries, while dbt on Amazon Redshift handles complex queries, unifying the development language and significantly reducing the technical learning curve. Using a single dbt modeling language not only simplifies the development process but also automatically generates consistent data lineage information. This approach offers robust adaptability, easily accommodating changes in data structures.</p> 
<p>By integrating Amazon Neptune graph database to store and analyze complex lineage relationships, combined with <a href="https://aws.amazon.com/step-functions/" rel="noopener" target="_blank">AWS Step Functions</a> and <a href="https://aws.amazon.com/lambda/" rel="noopener" target="_blank">AWS Lambda</a> functions, we achieve a fully automated data lineage generation process. This combination promotes consistency and completeness of lineage data while enhancing the efficiency and scalability of the entire process. The result is a powerful and flexible solution for end-to-end data lineage analysis.</p> 
<h2>Architecture overview</h2> 
<p>The experiment’s context involves a customer already using Amazon Athena for one-time queries. To better accommodate massive data processing and complex query scenarios, they aim to adopt a unified data modeling language across different data platforms. This led to the implementation of both Athena on dbt and Amazon Redshift on dbt architectures.</p> 
<p><a href="https://aws.amazon.com/glue/" rel="noopener" target="_blank">AWS Glue</a> crawler crawls data lake information from Amazon S3, generating a Data Catalog to support dbt on Amazon Athena data modeling. For complex query scenarios, AWS Glue performs extract, transform, and load (ETL) processing, loading data into the petabyte-scale data warehouse, Amazon Redshift. Here, data modeling uses dbt on Amazon Redshift.</p> 
<p>Lineage data original files from both parts are loaded into an S3 bucket, providing data support for end-to-end data lineage analysis.</p> 
<p>The following image is the architecture diagram for the solution.</p> 
<p><img alt="Figure 1-Architecture diagram of DBT modeling based on Athena and Redshift" class="alignnone size-full wp-image-73810" height="391" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-1-new-Architecture-diagram-of-DBT-modeling-based-on-Athena-and-Redshift.png.png" style="margin: 10px 0px 10px 0px;" width="781" /></p> 
<p>Some important considerations:</p> 
<ul> 
 <li>For implementing dbt modeling on Athena, refer to the <a href="https://github.com/jaehyeon-kim/dbt-on-aws/tree/main/athena" rel="noopener" target="_blank">dbt-on-aws / athena GitHub repository</a> for experimentation</li> 
 <li>For implementing dbt modeling on Amazon Redshift, refer to the <a href="https://github.com/jaehyeon-kim/dbt-on-aws/tree/main/redshift-sls" rel="noopener" target="_blank">dbt-on-aws / redshift GitHub repository</a> for experimentation.</li> 
</ul> 
<p>This experiment uses the following data dictionary:</p> 
<table border="1" cellpadding="10px" style="border-color: #000000;" width="633"> 
 <tbody> 
  <tr> 
   <td width="208"><strong>Source table</strong></td> 
   <td width="132"><strong>Tool</strong></td> 
   <td width="293"><strong>Target table</strong></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.name_basics</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__name_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.title_akas</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__title_akas</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.title_basics</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__title_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.title_crew</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__title_crews</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.title_episode</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__title_episodes</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.title_principals</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__title_principals</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>imdb.title_ratings</code></td> 
   <td width="132">DBT/Athena</td> 
   <td width="293"><code>stg_imdb__title_ratings</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__name_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__name_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__title_akas</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__title_akas</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__title_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__title_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__title_crews</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__title_crews</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__title_episodes</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__title_episodes</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__title_principals</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__title_principals</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>stg_imdb__title_ratings</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>new_stg_imdb__title_ratings</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__name_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>int_primary_profession_flattened_from_name_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__name_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>int_known_for_titles_flattened_from_name_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__name_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>names</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_akas</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>int_genres_flattened_from_title_basics</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_crews</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>int_directors_flattened_from_title_crews</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_crews</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>int_writers_flattened_from_title_crews</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_episodes</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_principals</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>new_stg_imdb__title_ratings</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>int_known_for_titles_flattened_from_name_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>int_primary_profession_flattened_from_name_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"></td> 
  </tr> 
  <tr> 
   <td width="208"><code>int_directors_flattened_from_title_crews</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>names</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>int_genres_flattened_from_title_basics</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>genre_titles</code></td> 
  </tr> 
  <tr> 
   <td width="208"><code>int_writers_flattened_from_title_crews</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"><code>names</code></td> 
  </tr> 
  <tr> 
   <td width="208">genre_titles</td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"></td> 
  </tr> 
  <tr> 
   <td width="208"><code>names</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"></td> 
  </tr> 
  <tr> 
   <td width="208"><code>titles</code></td> 
   <td width="132">DBT/Redshift</td> 
   <td width="293"></td> 
  </tr> 
 </tbody> 
</table> 
<p>The lineage data generated by dbt on Athena includes partial lineage diagrams, as exemplified in the following images. The first image shows the lineage of <code>name_basics</code> in dbt on Athena. The second image shows the lineage of <code>title_crew</code> in dbt on Athena.</p> 
<p><img alt="Figure 3-Lineage of name_basics in DBT on Athena" class="alignnone size-full wp-image-73811" height="714" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-3-Lineage-of-name_basics-in-DBT-on-Athena.png" style="margin: 10px 0px 10px 0px;" width="1542" /></p> 
<p><img alt="Figure 4-Lineage of title_crew in DBT on Athena" class="alignnone size-full wp-image-73812" height="714" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-4-Lineage-of-title_crew-in-DBT-on-Athena.png" style="margin: 10px 0px 10px 0px;" width="1542" /></p> 
<p>The lineage data generated by dbt on Amazon Redshift includes partial lineage diagrams, as illustrated in the following image.</p> 
<p><img alt="Figure 5-Lineage of name_basics and title_crew in DBT on Redshift" class="alignnone size-full wp-image-73813" height="714" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-5-Lineage-of-name_basics-and-title_crew-in-DBT-on-Redshift.png" style="margin: 10px 0px 10px 0px;" width="1542" /></p> 
<p>Referring to the data dictionary and screenshots, it’s evident that the complete data lineage information is highly dispersed, spread across 29 lineage diagrams. Understanding the end-to-end comprehensive view requires significant time. In real-world environments, the situation is often more complex, with complete data lineage potentially distributed across hundreds of files. Consequently, integrating a complete end-to-end data lineage diagram becomes crucial and challenging.</p> 
<p>This experiment will provide a detailed introduction to processing and merging data lineage files stored in Amazon S3, as illustrated in the following diagram.</p> 
<p><img alt="Figure 6-Merging data lineage from Athena and Redshift into Neptune" class="alignnone size-full wp-image-73814" height="411" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-6-new-Merging-data-lineage-from-Athena-and-Redshift-into-Neptune.png" style="margin: 10px 0px 10px 0px;" width="621" /></p> 
<h2>Prerequisites</h2> 
<p>To perform the solution, you need to have the following prerequisites in place:</p> 
<ul> 
 <li>The Lambda function for preprocessing lineage files must have permissions to access Amazon S3 and Amazon Redshift.</li> 
 <li>The Lambda function for constructing the <a href="https://en.wikipedia.org/wiki/Directed_acyclic_graph" rel="noopener" target="_blank">directed acyclic graph</a> (DAG) must have permissions to access Amazon S3 and Amazon Neptune.</li> 
</ul> 
<h2>Solution walkthrough</h2> 
<p>To perform the solution, follow the steps in the next sections.</p> 
<h3>Preprocess raw lineage data for DAG generation using Lambda functions</h3> 
<p>Use Lambda to preprocess the raw lineage data generated by dbt, converting it into key-value pair JSON files that are easily understood by Neptune: <code>athena_dbt_lineage_map.json</code> and <code>redshift_dbt_lineage_map.json</code>.</p> 
<ol> 
 <li>To create a new Lambda function in the Lambda console, enter a <strong>Function name</strong>, select the <strong>Runtime</strong> (Python in this example), configure the <strong>Architecture</strong> and <strong>Execution role</strong>, then click the “<strong>Create function</strong>” button.</li> 
</ol> 
<p><img alt="Figure 7-Basic configuration of athena-data-lineage-process Lambda" class="alignnone size-full wp-image-73815" height="647" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-7-Basic-configuration-of-athena-data-lineage-process-Lambda.png" style="margin: 10px 0px 10px 0px;" width="1102" /></p> 
<ol start="2"> 
 <li>Open the created Lambda function and on the <strong>Configuration</strong> tab, in the navigation pane, select <strong>Environment variables</strong> and choose your configurations. Using Athena on dbt processing as an example, configure the environment variables as follows (the process for Amazon Redshift on dbt is similar): 
  <ul> 
   <li><code>INPUT_BUCKET</code>: <code>data-lineage-analysis-24-09-22</code> (replace with the S3 bucket path storing the original Athena on dbt lineage files)</li> 
   <li><code>INPUT_KEY</code>: <code>athena_manifest.json</code> (the original Athena on dbt lineage file)</li> 
   <li><code>OUTPUT_BUCKET</code>: <code>data-lineage-analysis-24-09-22</code> (replace with the S3 bucket path for storing the preprocessed output of Athena on dbt lineage files)</li> 
   <li><code>OUTPUT_KEY</code>: <code>athena_dbt_lineage_map.json</code> (the output file after preprocessing the original Athena on dbt lineage file)</li> 
  </ul> </li> 
</ol> 
<p><img alt="Figure 8-Environment variable configuration for athena-data-lineage-process-Lambda" class="alignnone size-full wp-image-73816" height="304" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-8-Environment-variable-configuration-for-athena-data-lineage-process-Lambda.png" style="margin: 10px 0px 10px 0px;" width="1102" /></p> 
<ol start="3"> 
 <li>On the <strong>Code</strong> tab, in the lambda_function.py file, enter the preprocessing code for the raw lineage data. Here’s a code reference using Athena on dbt processing as an example (the process for Amazon Redshift on dbt is similar). The preprocessing code for Athena on dbt’s original lineage file is as follows:</li> 
</ol> 
<p>The <code>athena_manifest.json</code>, <code>redshift_manifest.json</code>, and other files used in this experiment can be obtained from the <a href="https://github.com/Honeyfish20/data-lineage-on-aws-2024" rel="noopener" target="_blank">Data Lineage Graph Construction GitHub repository</a>.</p> 
<div class="hide-language"> 
 <div class="hide-language"> 
  <div class="hide-language"> 
   <pre><code class="lang-python">import json
import boto3
import os

def lambda_handler(event, context):
    # Set up S3 client
    s3 = boto3.client('s3')

    # Get input and output paths from environment variables
    input_bucket = os.environ['INPUT_BUCKET']
    input_key = os.environ['INPUT_KEY']
    output_bucket = os.environ['OUTPUT_BUCKET']
    output_key = os.environ['OUTPUT_KEY']

    # Define helper function
    def dbt_nodename_format(node_name):
        return node_name.split(".")[-1]

    # Read input JSON file from S3
    response = s3.get_object(Bucket=input_bucket, Key=input_key)
    file_content = response['Body'].read().decode('utf-8')
    data = json.loads(file_content)
    lineage_map = data["child_map"]
    node_dict = {}
    dbt_lineage_map = {}

    # Process data
    for item in lineage_map:
        lineage_map[item] = [dbt_nodename_format(child) for child in lineage_map[item]]
        node_dict[item] = dbt_nodename_format(item)

    # Update key names
    lineage_map = {node_dict[old]: value for old, value in lineage_map.items()}
    dbt_lineage_map["lineage_map"] = lineage_map

    # Convert result to JSON string
    result_json = json.dumps(dbt_lineage_map)

    # Write JSON string to S3
    s3.put_object(Body=result_json, Bucket=output_bucket, Key=output_key)
    print(f"Data written to s3://{output_bucket}/{output_key}")

    return {
        'statusCode': 200,
        'body': json.dumps('Athena data lineage processing completed successfully')
    }</code></pre> 
  </div> 
 </div> 
</div> 
<h3>Merge preprocessed lineage data and write to Neptune using Lambda functions</h3> 
<ol> 
 <li>Before processing data with the Lambda function, create a Lambda layer by uploading the required Gremlin plugin. For detailed steps on creating and configuring Lambda <strong>Layers</strong>, see the <a href="https://docs.aws.amazon.com/lambda/latest/dg/chapter-layers.html" rel="noopener" target="_blank">AWS Lambda Layers documentation</a>.</li> 
</ol> 
<p>Because connecting Lambda to Neptune for constructing a DAG requires the Gremlin plugin, it needs to be uploaded before using Lambda. The Gremlin package can be obtained from the <a href="https://github.com/Honeyfish20/data-lineage-on-aws-2024" rel="noopener" target="_blank">Data Lineage Graph Construction GitHub repository</a>.</p> 
<p><img alt="Figure 9-Lambda layers" class="alignnone size-full wp-image-73817" height="163" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-9-Lambda-layers.png" style="margin: 10px 0px 10px 0px;" width="1597" /></p> 
<ol start="2"> 
 <li>Create a new Lambda function. Choose the function to configure. To the recently created layer, at the bottom of the page, choose <strong>Add a layer</strong>.</li> 
</ol> 
<p><img alt="Figure 10_Add a layer" class="alignnone size-full wp-image-73818" height="124" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-10-New_Add-a-layer.png" style="margin: 10px 0px 10px 0px;" width="1102" /></p> 
<p>Create another Lambda layer for the requests library, similar to how you created the layer for the Gremlin plugin. This library will be used for HTTP client functionality in the Lambda function.</p> 
<ol start="3"> 
 <li>Choose the recently created Lambda function to configure. Connect to Neptune through Lambda to merge the two datasets and construct a DAG. On the <strong>Code</strong> tab, the reference code to execute is as follows:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-python">import json
import boto3
import os
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials
from botocore.session import Session
from concurrent.futures import ThreadPoolExecutor, as_completed

def read_s3_file(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data.get("lineage_map", {})
    except Exception as e:
        print(f"Error reading S3 file {bucket}/{key}: {str(e)}")
        raise

def merge_data(athena_data, redshift_data):
    return {**athena_data, **redshift_data}

def sign_request(request):
    credentials = get_credentials(Session())
    auth = SigV4Auth(credentials, 'neptune-db', os.environ['AWS_REGION'])
    auth.add_auth(request)
    return dict(request.headers)

def send_request(url, headers, data):
    try:
        response = requests.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {str(e)}")
        if hasattr(e.response, 'text'):
            print(f"Response content: {e.response.text}")
        raise

def write_to_neptune(data):
    endpoint = 'https://your neptune endpoint name:8182/gremlin'
    # replace with your neptune endpoint name

    # Clear Neptune database
    clear_query = "g.V().drop()"
    request = AWSRequest(method='POST', url=endpoint, data=json.dumps({'gremlin': clear_query}))
    signed_headers = sign_request(request)
    response = send_request(endpoint, signed_headers, json.dumps({'gremlin': clear_query}))
    print(f"Clear database response: {response}")

    # Verify if the database is empty
    verify_query = "g.V().count()"
    request = AWSRequest(method='POST', url=endpoint, data=json.dumps({'gremlin': verify_query}))
    signed_headers = sign_request(request)
    response = send_request(endpoint, signed_headers, json.dumps({'gremlin': verify_query}))
    print(f"Vertex count after clearing: {response}")
    
    def process_node(node, children):
        # Add node
        query = f"g.V().has('lineage_node', 'node_name', '{node}').fold().coalesce(unfold(), addV('lineage_node').property('node_name', '{node}'))"
        request = AWSRequest(method='POST', url=endpoint, data=json.dumps({'gremlin': query}))
        signed_headers = sign_request(request)
        response = send_request(endpoint, signed_headers, json.dumps({'gremlin': query}))
        print(f"Add node response for {node}: {response}")

        for child_node in children:
            # Add child node
            query = f"g.V().has('lineage_node', 'node_name', '{child_node}').fold().coalesce(unfold(), addV('lineage_node').property('node_name', '{child_node}'))"
            request = AWSRequest(method='POST', url=endpoint, data=json.dumps({'gremlin': query}))
            signed_headers = sign_request(request)
            response = send_request(endpoint, signed_headers, json.dumps({'gremlin': query}))
            print(f"Add child node response for {child_node}: {response}")

            # Add edge
            query = f"g.V().has('lineage_node', 'node_name', '{node}').as('a').V().has('lineage_node', 'node_name', '{child_node}').coalesce(inE('lineage_edge').where(outV().as('a')), addE('lineage_edge').from('a').property('edge_name', ' '))"
            request = AWSRequest(method='POST', url=endpoint, data=json.dumps({'gremlin': query}))
            signed_headers = sign_request(request)
            response = send_request(endpoint, signed_headers, json.dumps({'gremlin': query}))
            print(f"Add edge response for {node} -&gt; {child_node}: {response}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_node, node, children) for node, children in data.items()]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in processing node: {str(e)}")

def lambda_handler(event, context):
    # Initialize S3 client
    s3_client = boto3.client('s3')

    # S3 bucket and file paths
    bucket_name = 'data-lineage-analysis' # Replace with your S3 bucket name
    athena_key = 'athena_dbt_lineage_map.json' # Replace with your athena lineage key value output json name
    redshift_key = 'redshift_dbt_lineage_map.json' # Replace with your redshift lineage key value output json name

    try:
        # Read Athena lineage data
        athena_data = read_s3_file(s3_client, bucket_name, athena_key)
        print(f"Athena data size: {len(athena_data)}")

        # Read Redshift lineage data
        redshift_data = read_s3_file(s3_client, bucket_name, redshift_key)
        print(f"Redshift data size: {len(redshift_data)}")

        # Merge data
        combined_data = merge_data(athena_data, redshift_data)
        print(f"Combined data size: {len(combined_data)}")

        # Write to Neptune (including clearing the database)
        write_to_neptune(combined_data)

        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully written to Neptune')
        }
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }</code></pre> 
</div> 
<h3>Create Step Functions workflow</h3> 
<ol> 
 <li>On the Step Functions console, choose <strong>State machines</strong>, and then choose <strong>Create state machine</strong>. On the <strong>Choose a template</strong> page, select <strong>Blank template</strong>.</li> 
</ol> 
<p><img alt="Figure 11-Step Functions blank template" class="alignnone size-full wp-image-73819" height="529" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-11-Step-Functions-blank-template.png" style="margin: 10px 0px 10px 0px;" width="1542" /></p> 
<ol start="2"> 
 <li>In the <strong>Blank template</strong>, choose <strong>Code</strong> to define your state machine. Use the following example code:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-json">{
  "Comment": "Daily Data Lineage Processing Workflow",
  "StartAt": "Parallel Processing",
  "States": {
    "Parallel Processing": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "Process Athena Data",
          "States": {
            "Process Athena Data": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "athena-data-lineange-process-Lambda", ##Replace with your Athena data lineage process Lambda function name
                "Payload": {
                  "input.$": "$"
                }
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "Process Redshift Data",
          "States": {
            "Process Redshift Data": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "redshift-data-lineange-process-Lambda", ##Replace with your Redshift data lineage process Lambda function name
                "Payload": {
                  "input.$": "$"
                }
              },
              "End": true
            }
          }
        }
      ],
      "Next": "Load Data to Neptune"
    },
    "Load Data to Neptune": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "data-lineage-analysis-lambda" ##Replace with your Lambda function Name
      },
      "End": true
    }
  }
}</code></pre> 
</div> 
<ol start="3"> 
 <li>After completing the configuration, choose the <strong>Design</strong> tab to view the workflow shown in the following diagram.</li> 
</ol> 
<p><img alt="Figure 12-Step Functions design view" class="alignnone size-full wp-image-73820" height="413" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-12-Step-Functions-design-view.png" style="margin: 10px 0px 10px 0px;" width="521" /></p> 
<h3>Create scheduling rules with Amazon EventBridge</h3> 
<p>Configure Amazon EventBridge to generate lineage data daily during off-peak business hours. To do this:</p> 
<ol> 
 <li>Create a new rule in the EventBridge console with a descriptive name.</li> 
 <li>Set the rule type to “<strong>Schedule</strong>” and configure it to run once daily (using either a fixed rate or the Cron expression “0 0 * * ? *”).</li> 
 <li>Select the AWS Step Functions state machine as the target and specify the state machine you created earlier.</li> 
</ol> 
<h3>Query results in Neptune</h3> 
<ol> 
 <li>On the Neptune console, select <strong>Notebooks</strong>. Open an existing notebook or create a new one.</li> 
</ol> 
<p><img alt="Figure 13-Neptune notebook" class="alignnone size-full wp-image-73821" height="465" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-13-Neptune-notebook.png" style="margin: 10px 0px 10px 0px;" width="1591" /></p> 
<ol start="2"> 
 <li>In the notebook, create a new code cell to perform a query. The following code example shows the query statement and its results:</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-code">%%gremlin -d node_name -de edge_name
g.V().hasLabel('lineage_node').outE('lineage_edge').inV().hasLabel('lineage_node').path().by(elementMap())</code></pre> 
</div> 
<p>You can now see the end-to-end data lineage graph information for both dbt on Athena and dbt on Amazon Redshift. The following image shows the merged DAG data lineage graph in Neptune.</p> 
<p><img alt="Figure 14-Merged DAG data lineage graph in Neptune" class="alignnone size-full wp-image-73822" height="892" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/07/Figure-14-Merged-DAG-data-lineage-graph-in-Neptune.png" style="margin: 10px 0px 10px 0px;" width="1591" /></p> 
<p>You can query the generated data lineage graph for data related to a specific table, such as title_crew.</p> 
<p>The sample query statement and its results are shown in the following code example:</p> 
<div class="hide-language"> 
 <pre><code class="lang-code">%%gremlin -d node_name -de edge_name
g.V().has('lineage_node', 'node_name', 'title_crew')
  .repeat(
    union(
      __.inE('lineage_edge').outV(),
      __.outE('lineage_edge').inV()
    )
  )
  .until(
    __.has('node_name', within('names', 'genre_titles', 'titles'))
    .or()
    .loops().is(gt(10))
  )
  .path()
  .by(elementMap())</code></pre> 
</div> 
<p>The following image shows the filtered results based on title_crew table in Neptune.</p> 
<p><img alt="Figure 15-Filtered results based on title_crew table in Neptune" class="alignnone size-full wp-image-73823" height="873" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/08/Figure-15-new-Filtered-results-based-on-title_crew-table-in-Neptune.png" style="margin: 10px 0px 10px 0px;" width="1571" /></p> 
<h2>Clean up</h2> 
<p>To clean up your resources, complete the following steps:</p> 
<ol> 
 <li>Delete EventBridge rules</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash"># Stop new events from triggering while removing dependencies
aws events disable-rule --name &lt;rule-name&gt;
# Break connections between rule and targets (like Lambda functions)
aws events remove-targets --rule &lt;rule-name&gt; --ids &lt;target-id&gt;
# Remove the rule completely from EventBridge
aws events delete-rule --name &lt;rule-name&gt;</code></pre> 
</div> 
<ol start="2"> 
 <li>Delete Step Functions state machine</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash"># Stop all running executions
aws stepfunctions stop-execution --execution-arn &lt;execution-arn&gt;
# Delete the state machine
aws stepfunctions delete-state-machine --state-machine-arn &lt;state-machine-arn&gt;</code></pre> 
</div> 
<ol start="3"> 
 <li>Delete Lambda functions</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash"># Delete Lambda function
aws lambda delete-function --function-name &lt;function-name&gt;
# Delete Lambda layers (if used)
aws lambda delete-layer-version --layer-name &lt;layer-name&gt; --version-number &lt;version&gt;</code></pre> 
</div> 
<ol start="4"> 
 <li>Clean up the Neptune database</li> 
</ol> 
<div class="hide-language"> 
 <pre><code class="lang-bash"># Delete all snapshots
aws neptune delete-db-cluster-snapshot --db-cluster-snapshot-identifier &lt;snapshot-id&gt;
# Delete database instance
aws neptune delete-db-instance --db-instance-identifier &lt;instance-id&gt; --skip-final-snapshot
# Delete database cluster
aws neptune delete-db-cluster --db-cluster-identifier &lt;cluster-id&gt; --skip-final-snapshot</code></pre> 
</div> 
<ol start="5"> 
 <li>Follow the instructions at <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-objects.html" rel="noopener" target="_blank">Deleting a single object</a> to clean up the S3 buckets</li> 
</ol> 
<h2>Conclusion</h2> 
<p>In this post, we demonstrated how dbt enables unified data modeling across Amazon Athena and Amazon Redshift, integrating data lineage from both one-time and complex queries. By using Amazon Neptune, this solution provides comprehensive end-to-end lineage analysis. The architecture uses AWS serverless computing and managed services, including Step Functions, Lambda, and EventBridge, providing a highly flexible and scalable design.</p> 
<p>This approach significantly lowers the learning curve through a unified data modeling method while enhancing development efficiency. The end-to-end data lineage graph visualization and analysis not only strengthen data governance capabilities but also offer deep insights for decision-making.</p> 
<p>The solution’s flexible and scalable architecture effectively optimizes operational costs and improves business responsiveness. This comprehensive approach balances technical innovation, data governance, operational efficiency, and cost-effectiveness, thus supporting long-term business growth with the adaptability to meet evolving enterprise needs.</p> 
<p>With OpenLineage-compatible data lineage now&nbsp;<a href="https://aws.amazon.com/blogs/aws/announcing-the-general-availability-of-data-lineage-in-the-next-generation-of-amazon-sagemaker-and-amazon-datazone/">generally available in Amazon DataZone</a>, we plan to explore integration possibilities to further enhance the system’s capability to handle complex data lineage analysis scenarios.</p> 
<p>If you have any questions, please feel free to leave a comment in the comments section.</p> 
<hr /> 
<h3>About the authors</h3> 
<p style="clear: both;"><img alt="nancynwu+photo" class="size-thumbnail wp-image-73836 alignleft" height="100" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/08/nancynwu-67x100.jpg" width="67" /></p> 
<p><strong>Nancy Wu</strong> is a Solutions Architect at AWS, responsible for cloud computing architecture consulting and design for multinational enterprise customers. Has many years of experience in big data, enterprise digital transformation research and development, consulting, and project management across telecommunications, entertainment, and financial industries.</p> 
<p style="clear: both;"><img alt="Xu+Feng+Photo" class="size-thumbnail wp-image-73845 alignleft" height="100" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/08/XuFengPhoto-69x100.jpg" width="69" /><strong>Xu Feng</strong> is a Senior Industry Solution Architect at AWS, responsible for designing, building, and promoting industry solutions for the Media &amp; Entertainment and Advertising sectors, such as intelligent customer service and business intelligence. With 20 years of software industry experience, currently focused on researching and implementing generative AI and AI-powered data solutions.</p> 
<p style="clear: both;"><img alt="Xu+Da+Photo" class="size-thumbnail wp-image-73850 alignleft" height="100" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/12/08/XuDaPhoto-71x100.jpeg" width="71" /><strong>Xu Da</strong> is a Amazon Web Services (AWS) Partner Solutions Architect based out of Shanghai, China. He has more than 25 years of experience in IT industry, software development and solution architecture. He is passionate about collaborative learning, knowledge sharing, and guiding community in their cloud technologies journey.</p>
