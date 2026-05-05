---
title: "Modernize your legacy databases with AWS data lakes, Part 2: Build a data lake using AWS DMS data on Apache Iceberg"
url: "https://aws.amazon.com/blogs/big-data/modernize-your-legacy-databases-with-aws-data-lakes-part-2-build-a-data-lake-using-aws-dms-data-on-apache-iceberg/"
date: "Wed, 30 Oct 2024 20:15:02 +0000"
author: "Shaheer Mansoor"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>This is part two of a three-part series where we show how to build a data lake on AWS using a <a href="https://docs.aws.amazon.com/whitepapers/latest/build-modern-data-streaming-analytics-architectures/what-is-a-modern-data-architecture.html" rel="noopener" target="_blank">modern data architecture</a>. This post shows how to load data from a legacy database (SQL Server) into a transactional data lake (<a href="https://iceberg.apache.org/" rel="noopener" target="_blank">Apache Iceberg</a>) using <a href="https://aws.amazon.com/glue" rel="noopener" target="_blank">AWS Glue</a>. We show how to build data pipelines using AWS Glue jobs, optimize them for both cost and performance, and implement schema evolution to automate manual tasks. To review the first part of the series, where we load SQL Server data into <a href="http://aws.amazon.com/s3" rel="noopener" target="_blank">Amazon Simple Storage Service</a> (Amazon S3) using <a href="https://aws.amazon.com/dms/" rel="noopener" target="_blank">AWS Database Migration Service</a> (AWS DMS), see <a href="https://aws.amazon.com/blogs/database/modernize-your-legacy-databases-with-aws-data-lakes-part-1-migrate-sql-server-using-aws-dms/" rel="noopener" target="_blank">Modernize your legacy databases with AWS data lakes, Part 1: Migrate SQL Server using AWS DMS</a>.</p> 
<h2>Solution overview</h2> 
<p>In this post, we go over the process of building a data lake, providing the rationale behind the different decisions, and share best practices when building such a solution.</p> 
<p>The following diagram illustrates the different layers of the data lake.</p> 
<p><img alt="Overall Architecture" class="alignnone wp-image-70495 size-full" height="658" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/22/Overall-Architecture-image001.png" width="1429" /></p> 
<p>To load data into the data lake, <a href="https://aws.amazon.com/step-functions/" rel="noopener" target="_blank">AWS Step Functions</a> can define a workflow, <a href="https://aws.amazon.com/sqs/" rel="noopener" target="_blank">Amazon Simple Queue Service</a> (Amazon SQS) can track the order of incoming files, and AWS Glue jobs and the Data Catalog can be used create the data lake silver layer. AWS DMS produces files and writes these files to the bronze bucket (as we explained in Part 1).</p> 
<p>We can turn on Amazon S3 notifications and push the new arriving file names to an SQS first-in-first-out (FIFO) queue. A Step Functions state machine can consume messages from this queue to process the files in the order they arrive.</p> 
<p>For processing the files, we need to create two types of AWS Glue jobs:</p> 
<ul> 
 <li><strong>Full load</strong> – This job loads the entire table data dump into an Iceberg table. Data types from the source are mapped to an Iceberg data type. After the data is loaded, the job updates the Data Catalog with the table schemas.</li> 
 <li><strong>CDC</strong> – This job loads the change data capture (CDC) files into the respective Iceberg tables. The AWS Glue job implements the schema evolution feature of Iceberg to handle schema changes such as addition or deletion of columns.</li> 
</ul> 
<p>As in Part 1, the AWS DMS jobs will place the full load and CDC data from the source database (SQL Server) in the raw S3 bucket. Now we process this data using AWS Glue and save it to the silver bucket in Iceberg format. AWS Glue has a plugin for Iceberg; for details, see <a href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html" rel="noopener" target="_blank">Using the Iceberg framework in AWS Glue</a>.</p> 
<p>Along with moving data from the bronze to the silver bucket, we also create and update the Data Catalog for further processing the data for the gold bucket.</p> 
<p>The following diagram illustrates how the full load and CDC jobs are defined inside the Step Functions workflow.</p> 
<p><img alt="Step Functions for loading data into the lake" class="alignnone wp-image-70498 size-full" height="1622" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/22/Step-Function-Workflow-image003.png" width="1430" /></p> 
<p>In this post, we discuss the AWS Glue jobs for defining the workflow. We recommend using <a href="https://docs.aws.amazon.com/step-functions/latest/dg/workflow-studio.html" rel="noopener" target="_blank">AWS Step Functions Workflow Studio</a>, and <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-event-notifications.html" rel="noopener" target="_blank">setting up Amazon S3 event notifications</a> and an <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html" rel="noopener" target="_blank">SNS FIFO queue</a> to receive the filename as messages.</p> 
<h2>Prerequisites</h2> 
<p>To follow the solution, you need the following prerequisites set up as well as certain access rights and <a href="http://aws.amazon.com/iam" rel="noopener" target="_blank">AWS Identity and Access Management</a> (IAM) privileges:</p> 
<ul> 
 <li>An <a href="https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html" rel="noopener" target="_blank">IAM role to run Glue jobs</a></li> 
 <li>IAM privileges to create AWS DMS resources (this role was created in <a href="https://aws.amazon.com/blogs/database/modernize-your-legacy-databases-with-aws-data-lakes-part-1-migrate-sql-server-using-aws-dms/" rel="noopener" target="_blank">Part 1</a> of this series; you can use the same role here)</li> 
 <li>The AWS DMS job from <a href="https://aws.amazon.com/blogs/database/modernize-your-legacy-databases-with-aws-data-lakes-part-1-migrate-sql-server-using-aws-dms/" rel="noopener" target="_blank">Part 1</a> working and producing files for the source database on Amazon S3.</li> 
</ul> 
<h2>Create an AWS Glue connection for the source database</h2> 
<p>We need to create a connection between AWS Glue and the source SQL Server database so the AWS Glue job can query the source for the latest schema while loading the data files. To create the connection, follow these steps:</p> 
<ol> 
 <li>On the AWS Glue console, choose <strong>Connections</strong> in the navigation pane.</li> 
 <li>Choose <strong>Create custom connector</strong>.</li> 
 <li>Give the connection a name and choose <strong>JDBC</strong> as the connection type.</li> 
 <li>In the <strong>JDBC URL </strong>section, enter the following string and replace the name of your source database endpoint and database that was set up in Part 1: <code>jdbc:sqlserver://{Your RDS End Point Name}:1433/{Your Database Name}</code>.</li> 
 <li>Select <strong>Require SSL connection</strong>, then choose <strong>Create connector</strong>.</li> 
</ol> 
<p><img alt="Clue Connections" class="alignnone wp-image-70493 size-full" height="420" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/22/Glue-Connections-image005.jpg" style="margin: 10px 0px 10px 0px;" width="794" /></p> 
<h2>Create and configure the full load AWS Glue job</h2> 
<p>Complete the following steps to create the full load job:</p> 
<ol> 
 <li>On the AWS Glue console, choose <strong>ETL jobs</strong> in the navigation pane.</li> 
 <li>Choose <strong>Script editor</strong> and select <strong>Spark</strong>.</li> 
 <li>Choose <strong>Start fresh</strong> and select <strong>Create script</strong>.</li> 
 <li>Enter a name for the full load job and choose the IAM role (mentioned in the prerequisites) for running the job.</li> 
 <li>Finish creating the job.</li> 
 <li>On the <strong>Job details</strong> tab, expand <strong>Advanced properties</strong>.</li> 
 <li>In the <strong>Connections</strong> section, add the connection you created.</li> 
 <li>Under <strong>Job parameters</strong>, pass the following arguments to the job: 
  <ol type="a"> 
   <li><strong>target_s3_bucket</strong> – The silver S3 bucket name.</li> 
   <li><strong>source_s3_bucket</strong> – The raw S3 bucket name.</li> 
   <li><strong>secret_id</strong> – The ID of the <a href="https://aws.amazon.com/secrets-manager/" rel="noopener" target="_blank">AWS Secrets Manager</a> secret for the source database credentials.</li> 
   <li><strong>dbname </strong>– The source database name.</li> 
   <li><strong>datalake-formats</strong> – This sets the data format to iceberg.</li> 
  </ol> </li> 
</ol> 
<p><img alt="" class="alignnone wp-image-71808 size-full" height="416" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/11/07/image-50.png" style="margin: 10px 0px 10px 0px;" width="1077" /></p> 
<p>The full load AWS Glue job starts after the AWS DMS task reaches 100%. The job loops over the files located in the raw S3 bucket and processes them one at time. For each file, the job infers the table name from the file name and gets the source table schema, including column names and primary keys.</p> 
<p>If the table has one or more primary keys, the job creates an equivalent Iceberg table. If the job has no primary key, the file is not processed. In our use case, all the tables have primary keys, so we enforce this check. Depending on your data, you might need to handle this scenario differently.</p> 
<p>You can use the following code to process the full load files. To start the job, choose <strong>Run</strong>.</p> 
<div class="hide-language"> 
 <pre><code class="lang-python">import sys, boto3, json
import boto3
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

#Get the arguments passed to the script
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                           'target_s3_bucket',
                           'secret_id',
                           'source_s3_bucket'])
dbname = "AdventureWorks"
schema = "HumanResources"

#Initialize parameters
target_s3_bucket = args['target_s3_bucket']
source_s3_bucket = args['source_s3_bucket']
secret_id = args['secret_id']
unprocessed_tables = []
drop_column_list = ['db', 'table_name', 'schema_name', 'Op', 'last_update_time']  # DMS added columns

#Helper Function: Get Credentials from Secrets Manager
def get_db_credentials(secret_id):
    secretsmanager = boto3.client('secretsmanager')
    response = secretsmanager.get_secret_value(SecretId=secret_id)
    secrets = json.loads(response['SecretString'])
    return secrets['host'], int(secrets['port']), secrets['username'], secrets['password']

#Helper Function: Load Iceberg table with Primary key(s)
def load_table(full_load_data_df, dbname, table_name):

    try:
        full_load_data_df = full_load_data_df.drop(*drop_column_list)
        full_load_data_df.createOrReplaceTempView('full_data')

        query = """
        CREATE TABLE IF NOT EXISTS glue_catalog.{0}.{1}
        USING iceberg
        LOCATION "s3://{2}/{0}/{1}"
        AS SELECT * FROM full_data
        """.format(dbname, table_name, target_s3_bucket)
        spark.sql(query)
        
        #Update Table property to accept Schema Changes
        spark.sql("""ALTER TABLE glue_catalog.{0}.{1} SET TBLPROPERTIES (
                      'write.spark.accept-any-schema'='true'
                    )""".format(dbname, table_name))
        
    except Exception as ex:
        print(ex)
        failed_table = {"table_name": table_name, "Reason": ex}
        unprocessed_tables.append(failed_table)
        
def get_table_key(host, port, username, password, dbname):
    
    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(host, port, dbname)
    
    connectionProperties = {
      "user" : username,
      "password" : password
    }
    
    spark.read.jdbc(url=jdbc_url, table='INFORMATION_SCHEMA.TABLE_CONSTRAINTS', properties=connectionProperties).createOrReplaceTempView("TABLE_CONSTRAINTS")
    spark.read.jdbc(url=jdbc_url, table='INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE', properties=connectionProperties).createOrReplaceTempView("CONSTRAINT_COLUMN_USAGE")
    df_table_pkeys = spark.sql("select c.TABLE_NAME, C.COLUMN_NAME as primary_key FROM TABLE_CONSTRAINTS T JOIN CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE T.CONSTRAINT_TYPE='PRIMARY KEY'")
    return df_table_pkeys


#Setup Spark configuration for reading and writing Iceberg tables
spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://{0}".format(dbname))
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)


#Initialize MSSQL credentials
host, port, username, password = get_db_credentials(secret_id)

#Initialize primary keys for all tables
df_table_pkeys = get_table_key(host, port, username, password, dbname)

#Read Full load csv files from s3
s3 = boto3.client('s3')
full_load_tables = s3.list_objects_v2(Bucket=source_s3_bucket, Prefix="raw/{0}/{1}".format(args['dbname'], args['schema']))

#Loop over files
for item in full_load_tables['Contents']:
    pkey_list = []
    table_name = item["Key"].split("/")[3].lower()
    print("Table name {0}".format(table_name))
    current_table_df = df_table_pkeys.where(df_table_pkeys.TABLE_NAME == table_name)

    # Only Process tables with at least 1 Primary key
    if not current_table_df.isEmpty():
        for i in current_table_df.collect():
            pkey_list.append(i["primary_key"])
    else:
        failed_table = {"table_name": table_name, "Reason": "No primary key"}
        unprocessed_tables.append(failed_table)
        # ToDo Handle these cases

    full_data_path = "s3://{0}/{1}".format(source_s3_bucket, item['Key'])
    full_load_data_df = (spark
                        .read
                        .option("header", True)
                        .option("inferSchema", True)
                        .option("recursiveFileLookup", "true")
                        .csv(full_data_path)
                        )

    primary_key = ",".join(pkey_list)

    if table_name not in unprocessed_tables:
        load_table(full_load_data_df, dbname, table_name)
</code></pre> 
</div> 
<p>When the job is complete, it creates the database and tables in the Data Catalog, as shown in the following screenshot.</p> 
<p><img alt="Data lake silver layer data" class="alignnone wp-image-70542 size-full" height="555" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/23/Data-lake-silver-layer-image007-1.png" style="margin: 10px 0px 10px 0px;" width="1793" /></p> 
<h2>Create and configure the CDC AWS Glue job</h2> 
<p>The CDC AWS Glue job is created similar to the full load job. As with the full load AWS Glue job, you need to use the source database connection and pass the job parameters with one additional parameter, <code>cdc_file</code>, which contains the location of the CDC file to be processed. Because a CDC file can contain data for multiple tables, the job loops over the tables in a file and loads the table metadata from the source table ( RDS column names).</p> 
<p>If the CDC operation is DELETE, the job deletes the records from the Iceberg table. If the CDC operation is INSERT or UPDATE, the job merges the data into the Iceberg table.</p> 
<p>You can use the following code to process the CDC files. To start the job, choose <strong>Run</strong></p> 
<div class="hide-language"> 
 <pre><code class="lang-python">import sys
import boto3
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Get the arguments passed to the script
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                           'target_s3_bucket',
                           'secret_id',
                           'source_s3_bucket',
                           'cdc_file'])
dbname = "AdventureWorks"
schema = "HumanResources"
target_s3_bucket = args['target_s3_bucket']
source_s3_bucket = args['source_s3_bucket']
secret_id = args['secret_id']
cdc_file = args['cdc_file']
unprocessed_tables = []
drop_column_list = ['db', 'table_name', 'schema_name', 'Op', 'last_update_time']  # DMS added columns
source_s3_cdc_file_key = "raw/AdventureWorks/cdc/" + cdc_file



# Helper Function: Get Credentials from Secrets Manager
def get_db_credentials(secret_id):
    secretsmanager = boto3.client('secretsmanager')
    response = secretsmanager.get_secret_value(SecretId=secret_id)
    secrets = json.loads(response['SecretString'])
    return secrets['host'], int(secrets['port']), secrets['username'], secrets['password']

# Helper Function: Column names from RDS
def get_table_colums(table, host, port, username, password, dbname):

    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(host, port, dbname)
    
    connectionProperties = {
      "user" : username,
      "password" : password
    }
    
    spark.read.jdbc(url=jdbc_url, table='INFORMATION_SCHEMA.COLUMNS', properties= connectionProperties).createOrReplaceTempView("TABLE_COLUMNS")
    columns = list((row.COLUMN_NAME) for (index, row) in spark.sql("select TABLE_NAME, TABLE_CATALOG, COLUMN_NAME from TABLE_COLUMNS where TABLE_NAME = '{0}' and TABLE_CATALOG = '{1}'".format(table, dbname)).select("COLUMN_NAME").toPandas().iterrows())
    return columns

# Helper Function: Get Colum names and datatypes from RDS
def get_table_colum_datatypes(table, host, port, username, password, dbname):

    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(host, port, dbname)
    
    connectionProperties = {
      "user" : username,
      "password" : password
    }
    
    spark.read.jdbc(url=jdbc_url, table='INFORMATION_SCHEMA.COLUMNS', properties= connectionProperties).createOrReplaceTempView("TABLE_COLUMNS")
    return spark.sql("select TABLE_NAME, COLUMN_NAME, DATA_TYPE from TABLE_COLUMNS WHERE TABLE_NAME ='{0}'".format(table))

# Helper Function: Setup the primary key condition
def get_iceberg_table_condition(database, tablename):
    
    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(host, port, database)
    
    connectionProperties = {
      "user" : username,
      "password" : password
    }
    
    spark.read.jdbc(url=jdbc_url, table='INFORMATION_SCHEMA.TABLE_CONSTRAINTS', properties=connectionProperties).createOrReplaceTempView("TABLE_CONSTRAINTS")
    spark.read.jdbc(url=jdbc_url, table='INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE', properties=connectionProperties).createOrReplaceTempView("CONSTRAINT_COLUMN_USAGE")
    
    condition = ''
    
    for key in spark.sql("select C.COLUMN_NAME FROM TABLE_CONSTRAINTS T JOIN CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE T.CONSTRAINT_TYPE='PRIMARY KEY' AND c.TABLE_NAME = '{0}'".format(table)).collect():
        condition += "target.{0} = source.{0} and".format(key.COLUMN_NAME)
    return condition[:-4]

    
# Read incoming data from Amazon S3
def read_cdc_S3(source_s3_bucket, source_s3_cdc_file_key):
    
    inputDf = (spark
                    .read
                    .option("header", False)
                    .option("inferSchema", True)
                    .option("recursiveFileLookup", "true")
                    .csv("s3://" + source_s3_bucket + "/" + source_s3_cdc_file_key)
                    )
    return inputDf

# Setup Spark configuration for reading and writing Iceberg tables
spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://{0}".format(target_s3_bucket))
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

#Initialize MSSQL credentials
host, port, username, password = get_db_credentials(secret_id)

#Read the cdc file 
cdc_df = read_cdc_S3(source_s3_bucket, source_s3_cdc_file_key)

tables = cdc_df.toPandas()._c1.unique().tolist()

#Loop over tables in the cdc file
for table in tables:
    #Create dataframes for delets and for inserts and updates
    table_df_deletes = cdc_df.where((cdc_df._c1 == table) &amp; (cdc_df._c0 == "D")).drop(cdc_df.columns[0], cdc_df.columns[1], cdc_df.columns[2], cdc_df.columns[3])
    table_df_upserts = cdc_df.where((cdc_df._c1 == table) &amp; ((cdc_df._c0 == "I") | (cdc_df._c0 == "U"))).drop(cdc_df.columns[0], cdc_df.columns[1], cdc_df.columns[2], cdc_df.columns[3])
    
    #Update column names for the dataframes
    columns = get_table_colums(table, host, port, username, password, dbname) 
    selectExpr = [] 

    for column in columns: 
        selectExpr.append(cdc_df.where((cdc_df._c1 == table)).drop(cdc_df.columns[0], cdc_df.columns[1], cdc_df.columns[2], cdc_df.columns[3]).columns[columns.index(column)] + " as " + column)

    table_df_deletes = table_df_deletes.selectExpr(selectExpr) 
    table_df_upserts = table_df_upserts.selectExpr(selectExpr)
    
    #Process Deletes
    if table_df_deletes.count() &gt; 0:
        
        print("Delete Triggered")
        table_df_deletes.createOrReplaceTempView('deleted_rows')
        
        sql_string = """MERGE INTO glue_catalog.{0}.{1} target
                        USING (SELECT * FROM deleted_rows) source
                        ON {2}
                        WHEN MATCHED 
                        THEN DELETE""".format(database, table.lower(), get_iceberg_table_condition(database, table.lower()))
        spark.sql(sql_string)
    
    if table_df_upserts.count() &gt; 0:
        print("Upsert triggered")

        #Upsert Records when there are Schema Changes
        if len(table_df_upserts.columns) != len(columns):

            #Handle column deletes
            if len(table_df_upserts.columns) &lt; len(columns):

                drop_columns = list(set(columns) - set(table_df_upserts.columns))

                for drop_column in drop_columns:
                    sql_string = """
                                    ALTER TABLE glue_catalog.{0}.{1}
                                    DROP COLUMN {2}""".format(dbname.lower(), table.lower(), drop_column)
                    spark.sql(sql_string)

            #Handle column additions
            elif len(table_df_upserts.columns) &gt; len(columns):

                column_datatype_df = get_table_colum_datatypes(table, host, port, username, password, dbname)
                add_columns = list(set(table_df_upserts.columns) - set(columns))

                for add_column in add_columns:

                    #Set Iceberg data type
                    data_type = list((row.DATA_TYPE) for (index, row) in column_datatype_df.filter("COLUMN_NAME='{0}'".format(add_column)).select("DATA_TYPE").toPandas().iterrows())[0]

                    # Convert MSSQL Datatypes to Iceberg supported datatypes
                    if data_type.lower() in ["varchar", "char"]:
                        data_type = "string"

                    if data_type.lower() in ["bigint"]:
                        data_type = "long"

                    if data_type.lower() in ["array"]:
                        data_type = "list"

                    sql_string = """
                                    ALTER TABLE glue_catalog.{0}.{1}
                                    ADD COLUMN {2} {3}""".format(dbname.lower(), table.lower(), add_column, data_type)
                    spark.sql(sql_string)
                    
            #Create statement to update columns
            update_table_column_list = ""
            insert_column_list = ""
            columns = get_table_colums(table, host, port, username, password, dbname)             

            for column in columns:

                update_table_column_list+="""target.{0}=source.{0},""".format(column)
                insert_column_list+="""source.{0},""".format(column)

            table_df_upserts.createOrReplaceTempView('updated_rows')

            sql_string = """MERGE INTO glue_catalog.{0}.{1} target
                            USING (SELECT * FROM updated_rows) source
                            ON {2}
                            WHEN MATCHED 
                            THEN UPDATE SET {3} 
                            WHEN NOT MATCHED THEN INSERT ({4}) VALUES ({5})""".format(dbname.lower(), 
                                                                                      table.lower(), 
                                                                                      get_iceberg_table_condition(dbname.lower(), table.lower()), 
                                                                                      update_table_column_list.rstrip(","), 
                                                                                      ",".join(columns), 
                                                                                      insert_column_list.rstrip(","))

            spark.sql(sql_string)

    
print("CDC job complete")</code></pre> 
</div> 
<p>The Iceberg <code>MERGE INTO</code> syntax can handle cases where a new column is added. For more details on this feature, see the <a href="https://iceberg.apache.org/docs/1.5.0/spark-writes/#merge-into-syntax" rel="noopener" target="_blank">Iceberg MERGE INTO syntax documentation</a>. If the CDC job needs to process many tables in the CDC file, the job can be multi-threaded to process the file in parallel.</p> 
<p><strong>&nbsp;</strong></p> 
<h2>Configure EventBridge notifications, SQS queue, and Step Functions state machine</h2> 
<p>You can use <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventBridge.html" rel="noopener" target="_blank">EventBridge notifications</a> to send notifications to EventBridge when certain events occur on S3 buckets, such as when new objects are created and deleted. For this post, we’re interested in the events when new CDC files from AWS DMS arrive in the bronze S3 bucket. You can create event notifications for new objects and insert the file names into an SQS queue. A Lambda function within Step Functions would consume from the queue, extract the file name, start a CDC Glue job, and pass the file name as a parameter to the job.</p> 
<p>AWS DMS CDC files contain database insert, update, and delete statements. We need to process these in order, so we use an SQS FIFO queue, which preserves the order of messages in which they arrive. You can also configure Amazon SQS to set a time to live (TTL); this parameter defines how long a message stays in the queue before it expires.</p> 
<p>Another important parameter to consider when configuring an SQS queue is the <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html" rel="noopener" target="_blank">message visibility timeout value</a>. While a message is being processed, it disappears from the queue to make sure that the message isn’t consumed by multiple consumers (AWS Glue jobs in our case). If the message is consumed successfully, it should be deleted from the queue before the visibility timeout. However, if the visibility timeout expires and the message isn’t deleted, the message reappears in the queue. In our solution, this timeout must be greater than the time it takes for the CDC job to process a file.</p> 
<p>Lastly, we recommend using Step Functions to define a workflow for handling the full load and CDC files. Step Functions has <a href="https://docs.aws.amazon.com/step-functions/latest/dg/integrate-services.html" rel="noopener" target="_blank">built-in integrations</a> to other AWS services like Amazon SQS, AWS Glue, and Lambda, which makes it a good candidate for this use case.</p> 
<p>The Step Functions state machine starts with checking the status of the AWS DMS task. The AWS DMS tasks can be <a href="https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Monitoring.html" rel="noopener" target="_blank">queried to check the status of the full load</a>, and we check the value of the parameter <code>FullLoadProgressPercent</code>. When this value gets to 100%, we can start processing the full load files. After the AWS Glue job processes the full load files, we start <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html" rel="noopener" target="_blank">polling the SQS queue</a> to check the size of the queue. If the queue size is greater than 0, this means new CDC files have arrived and we can start the AWS Glue CDC job to process these files. The AWS Glue jobs processes the CDC files and deletes the messages from the queue. When the queue size reaches 0, the AWS Glue job exits and we loop in the Step Functions workflow to check the SQS queue size.</p> 
<p>Because the Step Functions state machine is supposed to run indefinitely, it’s good to keep in mind that there will be service limits you need to adhere to. Namely, the maximum runtime, which is 1 year, and maximum run history size, i.e., state transitions or events for a state machine which is 25,000. We recommend adding an additional step at the end to check if either of these conditions are being met to stop the current state machine run and start a new one.</p> 
<p>The following diagram illustrates how you can use Step Functions state machine history size to monitor and start a new Step Functions state machine run.</p> 
<p><img alt="Step Functions Workflow" class="alignnone wp-image-70499 size-full" height="1832" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/22/Step-Function-Workflow-image008.jpg" width="1041" /></p> 
<h2>Configure the pipeline</h2> 
<p>The pipeline needs to be configured to address cost, performance, and resilience goals. You might want a pipeline that can load fresh data into the data lake and make it available quickly, and you might also want to optimize costs by loading large chunks of data into the data lake. At the same time, you should make the pipeline resilient and be able to recover in case of failures. In this section, we cover the different parameters and recommended settings to achieve these goals.</p> 
<p>Step Functions is designed to process incoming AWS DMS CDC files by running AWS Glue jobs. AWS Glue jobs can take a couple of minutes to boot up, and when they’re running, it’s efficient to process large chunks of data. You can configure AWS DMS to write CSV files to Amazon S3 by configuring the following AWS DMS task parameters:</p> 
<ul> 
 <li><code>CdcMaxBatchInterval</code> – Defines the maximum time limit AWS DMS will wait before writing a batch to Amazon S3</li> 
 <li><code>CdcMinFileSize</code> – Defines the minimum file size AWS DMS will write to Amazon S3</li> 
</ul> 
<p>Whichever condition is met first will invoke the write operation. If you want to prioritize data freshness, you should have a short CdcMaxBatchInterval value (10 seconds) and a small CdcMinFileSize value (1–5 MB). This will result in many small CSV files being written to Amazon S3 and will invoke a lot of AWS Glue jobs to process the data, making the extract, transform, and load (ETL) process faster. If you want to optimize costs, you should have a moderate CdcMaxBatchInterval (minutes) and a large CdcMinFileSize value (100–500 MB). In this scenario, we start a few AWS Glue jobs that will process large chunks of data, making the ETL flow more efficient. In a real-world use case, the required values for these parameters might fall somewhere that’s a good compromise between throughput and cost. You can configure these parameters when creating a target endpoint using the AWS DMS console, or by using the create-endpoint command in the <a href="http://aws.amazon.com/cli" rel="noopener" target="_blank">AWS Command Line Interface</a> (AWS CLI).</p> 
<p>For the full list of parameters, see <a href="https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Configuring" rel="noopener" target="_blank">Using Amazon S3 as a target for AWS Database Migration Service</a>.</p> 
<p>Choosing the right AWS Glue worker types for the full load and CDC jobs is also crucial for performance and cost optimization. The AWS Glue (Spark) workers range from G1X to G8X, which have an increasing number of data processing units (DPUs). Full load files are usually much larger in size compared to CDC files, and therefore it’s more cost- and performance-effective to select a larger worker. For CDC files, it would be more cost-effective to select a smaller worker because files sizes are smaller.</p> 
<p>You should design the Step Functions state machine in such a way that if anything fails, the pipeline can be redeployed after repair and resume processing from where it left off. One important parameter here is TTL for the messages in the SQS queue. This parameter defines how long a message stays in the queue before expiring. In case of failures, we want this parameter to be long enough for us to deploy a fix. Amazon SQS has a maximum of 14 days for a message’s TTL. We recommend setting this to a large enough value to minimize messages being expired in case of pipeline failures.</p> 
<h2>Clean up</h2> 
<p>Complete the following steps to clean up the resources you created in this post:</p> 
<ol> 
 <li>Delete the AWS Glue jobs: 
  <ol type="a"> 
   <li>On the AWS Glue console, choose <strong>ETL jobs</strong> in the navigation pane.</li> 
   <li>Select the full load and CDC jobs and on the <strong>Actions</strong> menu, choose <strong>Delete</strong>.</li> 
   <li>Choose <strong>Delete</strong> to confirm.</li> 
  </ol> </li> 
 <li>Delete the Iceberg tables: 
  <ol type="a"> 
   <li>On the AWS Glue console, under <strong>Data Catalog</strong> in the navigation pane, choose <strong>Databases</strong>.</li> 
   <li>Choose the database in which the Iceberg tables reside.</li> 
   <li>Select the tables to delete, choose <strong>Delete</strong>, and confirm the deletion.</li> 
  </ol> </li> 
 <li>Delete the S3 bucket: 
  <ol type="a"> 
   <li>On the Amazon S3 console, choose <strong>Buckets</strong> in the navigation pane.</li> 
   <li>Choose the silver bucket and empty the files in the bucket.</li> 
   <li>Delete the bucket.</li> 
  </ol> </li> 
</ol> 
<h2>Conclusion</h2> 
<p>In this post, we showed how to use AWS Glue jobs to load AWS DMS files into a transactional data lake framework such as Iceberg. In our setup, AWS Glue provided highly scalable and simple-to-maintain ETL jobs. Furthermore, we share a proposed solution using Step Functions to create an ETL pipeline workflow, with Amazon S3 notifications and an SQS queue to capture newly arriving files. We shared how to design this system to be resilient towards failures and to automate one of the most time-consuming tasks in maintaining a data lake: schema evolution.</p> 
<p>In <a href="https://aws.amazon.com/blogs/big-data/modernize-your-legacy-databases-with-aws-data-lakes-part-3-build-a-data-lake-processing-layer/" rel="noopener" target="_blank">Part 3</a>, we will share how to process the data lake to create data marts.</p> 
<hr /> 
<h3>About the Authors</h3> 
<p style="clear: both;"><img alt="" class="wp-image-70637 size-full alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/23/bdb4079-3-author-5-1.jpg" width="100" /><strong>Shaheer Mansoor</strong> is a Senior Machine Learning Engineer at AWS, where he specializes in developing cutting-edge machine learning platforms. His expertise lies in creating scalable infrastructure to support advanced AI solutions. His focus areas are MLOps, feature stores, data lakes, model hosting, and generative AI.</p> 
<p style="clear: both;"><img alt="" class="wp-image-70640 size-full alignleft" height="133" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/23/bdb4079-3-author-6-scaled-1.jpg" width="100" /><strong>Anoop Kumar K M</strong> is a Data Architect at AWS with focus in the data and analytics area. He helps customers in building scalable data platforms and in their enterprise data strategy. His areas of interest are data platforms, data analytics, security, file systems and operating systems. Anoop loves to travel and enjoys reading books in the crime fiction and financial domains.</p> 
<p style="clear: both;"><img alt="" class="wp-image-70641 size-full alignleft" height="134" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2024/10/23/bdb4079-3-author-7-1.png" width="100" /><strong>Sreenivas Nettem</strong> is a Lead Database Consultant at AWS Professional Services. He has experience working with Microsoft technologies with a specialization in SQL Server. He works closely with customers to help migrate and modernize their databases to AWS.</p>
