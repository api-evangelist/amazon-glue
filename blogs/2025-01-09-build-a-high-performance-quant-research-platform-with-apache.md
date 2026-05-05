---
title: "Build a high-performance quant research platform with Apache Iceberg"
url: "https://aws.amazon.com/blogs/big-data/build-a-high-performance-quant-research-platform-with-apache-iceberg/"
date: "Thu, 09 Jan 2025 20:55:39 +0000"
author: "Guy Bachar"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
<p>In our previous post <a href="https://aws.amazon.com/blogs/big-data/backtesting-index-rebalancing-arbitrage-with-amazon-emr-and-apache-iceberg/" rel="noopener" target="_blank">Backtesting index rebalancing arbitrage with Amazon EMR and Apache Iceberg</a>, we showed how to use Apache Iceberg in the context of strategy backtesting. In this post, we focus on data management implementation options such as accessing data directly in <a href="https://aws.amazon.com/s3/" rel="noopener" target="_blank">Amazon Simple Storage Service</a> (Amazon S3), using popular data formats like Parquet, or using open table formats like Iceberg. Our experiments are based on real-world historical full order book data, provided by our partner <a href="https://cryptostruct.com/" rel="noopener" target="_blank">CryptoStruct</a>, and compare the trade-offs between these choices, focusing on performance, cost, and quant developer productivity.</p> 
<p>Data management is the foundation of quantitative research. Quant researchers spend approximately 80% of their time on necessary but not impactful data management tasks such as data ingestion, validation, correction, and reformatting. Traditional data management choices include relational, SQL, NoSQL, and specialized time series databases. In recent years, advances in parallel computing in the cloud have made object stores like Amazon S3 and columnar file formats like Parquet a preferred choice.</p> 
<p>This post explores how Iceberg can enhance quant research platforms by improving query performance, reducing costs, and increasing productivity, ultimately enabling faster and more efficient strategy development in quantitative finance. Our analysis shows that Iceberg can accelerate query performance by up to 52%, reduce operational costs, and significantly improve data management at scale.</p> 
<p>Having chosen Amazon S3 as our storage layer, a key decision is whether to access Parquet files directly or use an open table format like Iceberg. Iceberg offers distinct advantages through its metadata layer over Parquet, such as improved data management, performance optimization, and integration with various query engines.</p> 
<p>In this post, we use the term <em>vanilla Parquet</em> to refer to Parquet files stored directly in Amazon S3 and accessed through standard query engines like Apache Spark, without the additional features provided by table formats such as Iceberg.</p> 
<p><strong>Note</strong>: This benchmark uses Apache Iceberg with the AWS Glue Data Catalog and Amazon S3 for storage. If you are familiar with S3 Tables, AWS’s managed implementation of Apache Iceberg, then please note that while the underlying technology is similar, there are differences in implementation and management. S3 Tables offers a fully managed Iceberg experience, whereas our benchmarks use open-source Iceberg with AWS services. For more information on <a href="https://aws.amazon.com/s3/features/tables/" rel="noopener" target="_blank">Amazon S3 Tables</a> and its specific performance characteristics, refer to <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html" rel="noopener" target="_blank">this AWS blog post</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html" rel="noopener" target="_blank">documentation</a>.</p> 
<h2>Quant developer and researcher productivity</h2> 
<p>In this section, we focus on the productivity features offered by Iceberg and how it compares to directly reading files in Amazon S3. As mentioned earlier, 80% of quantitative research work is attributed to data management tasks. Business impact heavily relies on quality data (“garbage in, garbage out”). Quants and platform teams have to ingest data from multiple sources with different velocities and update frequencies, and then validate and correct the data. These activities translate into the ability to run append, insert, update, and delete operations. For simple append operations, both Parquet on Amazon S3 and Iceberg offer similar convenience and productivity. However, real-world data is never perfect and needs to be corrected. Gaps filling (inserts), error corrections and restatements (updates), and removing duplicates (deletes) are the most obvious examples. When writing data in the Parquet format directly to Amazon S3 without using an open table format like Iceberg, you have to write code to identify the affected partition, correct errors, and rewrite the partition. Moreover, if the write job fails or a downstream read job occurs during this write operation, all downstream jobs have the possibility of reading inconsistent data. However, Iceberg has built-in insert, update, and delete features with ACID (Atomicity, Consistency, Isolation, Durability) properties, and the framework itself manages the Amazon S3 mechanics on your behalf.</p> 
<p>Guarding against lookahead bias is an essential capability of any quant research platform—what backtests as a profitable trading strategy can render itself useless and unprofitable in real time. Iceberg provides time travel and snapshotting capabilities out of the box to manage lookahead bias that could be embedded in the data (such as delayed data delivery).</p> 
<h3>Simplified data corrections and updates</h3> 
<p>Iceberg enhances data management for quants in capital markets through its robust insert, delete, and update capabilities. These features allow efficient data corrections, gap-filling in time series, and historical data updates without disrupting ongoing analyses or compromising data integrity.</p> 
<p>Unlike direct Amazon S3 access, Iceberg supports these operations on petabyte-scale data lakes without requiring complex custom code. This simplifies data modification processes, which is crucial for ingesting and updating large volumes of market and trade data, quickly iterating on backtesting and reprocessing workflows, and maintaining detailed audit trails for risk and compliance requirements.</p> 
<p>Iceberg’s <a href="https://iceberg.apache.org/spec/" rel="noopener" target="_blank">table format</a> separates data files from metadata files, enabling efficient data modifications without full dataset rewrites. This approach also reduces expensive <code>ListObjects</code> API calls typically needed when directly accessing Parquet files in Amazon S3.</p> 
<p>Additionally, Iceberg offers merge on read (MoR) and copy on write (CoW) approaches, providing flexibility for different quant research needs. MoR enables faster writes, suitable for frequently updated datasets, and CoW provides faster reads, beneficial for read-heavy workflows like backtesting.</p> 
<p>For example, when a new data source or attribute is added, quant researchers can seamlessly incorporate it into their Iceberg tables and then reprocess historical data, confident they’re using correct, time-appropriate information. This capability is particularly valuable in maintaining the integrity of backtests and the reliability of trading strategies.</p> 
<p>In scenarios involving large-scale data corrections or updates, such as adjusting for stock splits or dividend payments across historical data, Iceberg’s efficient update mechanisms significantly reduce processing time and resource usage compared to traditional methods.</p> 
<p>These features collectively improve productivity and data management efficiency in quant research environments, allowing researchers to focus more on strategy development and less on data handling complexities.</p> 
<h3>Historical data access for backtesting and validation</h3> 
<p>Iceberg’s time travel feature can enable quant developers and researchers to access and analyze historical snapshots of their data. This capability can be useful while performing tasks like backtesting, model validation, and understanding data lineage.</p> 
<p>Iceberg simplifies time travel workflows on Amazon S3 by introducing a metadata layer that tracks the history of changes made to the table. You can refer to this metadata layer to create a mental model of how Iceberg’s time travel capability works.</p> 
<p>Iceberg’s time travel capability is driven by a concept called <em>snapshots</em>, which are recorded in metadata files. These metadata files act as a central repository that stores table metadata, including the history of snapshots. Additionally, Iceberg uses manifest files to provide a representation of data files, their partitions, and any associated deleted files. These manifest files are referenced in the metadata snapshots, allowing Iceberg to identify the relevant data for a specific point in time.</p> 
<p>When a user requests a time travel query, the typical workflow involves querying a specific snapshot. Iceberg uses the snapshot identifier to locate the corresponding metadata snapshot in the metadata files. The time travel capability is invaluable to quants, enabling them to backtest and validate strategies against historical data, reproduce and debug issues, perform what-if analysis, comply with regulations by maintaining audit trails and reproducing past states, and roll back and recover from data corruption or errors. Quants can also gain deeper insights into current market trends and correlate them with historical patterns. Also, the time travel feature can further mitigate any risks of lookahead bias. Researchers can access the exact data snapshots that were present in the past, and then run their models and strategies against this historical data, without the risk of inadvertently incorporating future information.</p> 
<h3>Seamless integration with familiar tools</h3> 
<p>Iceberg provides a variety of interfaces that enable seamless integration with the open source tools and AWS services that quant developers and researchers are familiar with.</p> 
<p>Iceberg provides a comprehensive SQL interface that allows quant teams to interact with their data using familiar SQL syntax. This SQL interface is compatible with popular query engines and data processing frameworks, such as Spark, Trino, <a href="https://aws.amazon.com/athena/" rel="noopener" target="_blank">Amazon Athena</a>, and Hive. Quant developers and researchers can use their existing SQL knowledge and tools to query, filter, aggregate, and analyze their data stored in Iceberg tables.</p> 
<p>In addition to the primary interface of SQL, Iceberg also provides the <a href="https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/iceberg-spark.html" rel="noopener" target="_blank">DataFrame API</a>, which allows quant teams to programmatically interact with their data with popular distributed data processing frameworks like Spark and Flink as well as thin clients like PyIceberg. Quants can further use this API to build more programmatic approaches to access and manipulate data, allowing for the implementation of custom logic and integration of Iceberg with other AWS ecosystems like <a href="https://aws.amazon.com/emr/" rel="noopener" target="_blank">Amazon EMR</a>.</p> 
<p>Although accessing data from Amazon S3 is a viable option, Iceberg provides several advantages like metadata management, performance optimization using partition pruning, data manipulation, and a rich AWS ecosystem integration including services like Athena and Amazon EMR with more seamless and feature-rich data processing experience.</p> 
<h2>Undifferentiated heavy lifting</h2> 
<p>Data partitioning is one of major contributing factors to optimizing aggregate throughput to and from Amazon S3, contributing to overall <a href="https://aws.amazon.com/hpc/" rel="noopener" target="_blank">High Performance Computing</a> (HPC) environment price-performance.</p> 
<p>Quant researchers often face performance bottlenecks and complex data management challenges when dealing with large-scale datasets in Amazon S3. As discussed in <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html" rel="noopener" target="_blank">Best practices design patterns: optimizing Amazon S3 performance</a>, single prefix performance is limited to 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix. Iceberg’s metadata layer and intelligent partitioning strategies automatically optimize data access patterns, reducing the likelihood of I/O throttling and minimizing the need for manual performance tuning. This automation allows quant teams to focus on developing and refining trading strategies rather than troubleshooting data access issues or optimizing storage layouts.</p> 
<p>In this section, we discuss situations we discovered while running our experiments at scale and solutions provided by Iceberg vs. vanilla Parquet when accessing data in Amazon S3.</p> 
<p>As we mentioned in the introduction, the nature of quant research is “fail fast”—new ideas have to be quickly evaluated and then either prioritized for a deep dive or dismissed. This makes it impossible to come up with universal partitioning that works all the time and for all research styles.</p> 
<p>When accessing data directly as Parquet files in Amazon S3, without using an open table format like Iceberg, partitioning and throttling issues can arise. Partitioning in this case is determined by the physical layout of files in Amazon S3, and a mismatch between the intended partitioning and the actual file layout can lead to I/O throttling exceptions. Additionally, listing directories in Amazon S3 can also result in throttling exceptions due to the high number of API calls required.</p> 
<p>In contrast, Iceberg provides a metadata layer that abstracts away the physical file layout in Amazon S3. Partitioning is defined at the table level, and Iceberg handles the mapping between logical partitions and the underlying file structure. This abstraction helps mitigate partitioning issues and reduces the likelihood of I/O throttling exceptions. Furthermore, Iceberg’s metadata caching mechanism minimizes the number of List API calls required, addressing the directory listing throttling issue.</p> 
<p>Although both approaches involve direct access to Amazon S3, Iceberg is an open table format that introduces a metadata layer, providing better partitioning management and reducing the risk of throttling exceptions. It doesn’t act as a database itself, but rather as a data format and processing engine on top of the underlying storage (in this case, Amazon S3).</p> 
<p>One of the most effective techniques to address Amazon S3 API quota limits is <em>salting</em> (random hash prefixes)—a method that adds random partition IDs to Amazon S3 paths. This increases the probability of prefixes residing on different physical partitions, helping distribute API requests more evenly. Iceberg supports this functionality out of the box for both data ingestion and reading.</p> 
<p>Implementing salting directly in Amazon S3 requires complex custom code to create and use partitioning schemes with random keys in the <a href="https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/" rel="noopener" target="_blank">naming hierarchy</a>. This approach necessitates a custom data catalog and metadata system to map physical paths to logical paths, allowing direct partition access without relying on Amazon S3 List API calls. Without such a system, applications risk exceeding Amazon S3 API quotas when accessing specific partitions.</p> 
<p>At petabyte scale, Iceberg’s advantages become clear. It efficiently manages data through the following features:</p> 
<ul> 
 <li>Directory caching</li> 
 <li>Configurable partitioning strategies (range, bucket)</li> 
 <li>Data management functionality (compaction)</li> 
 <li>Catalog, metadata, and statistics use for optimal execution plans</li> 
</ul> 
<p>These built-in features eliminate the need for custom solutions to manage Amazon S3 API quotas and data organization at scale, reducing development time and maintenance costs while improving query performance and reliability.</p> 
<h2>Performance</h2> 
<p>We highlighted a lot of the functionality of Iceberg that eliminates undifferentiated heavy lifting and improves developer and quant productivity. What about performance?</p> 
<p>This section evaluates whether Iceberg’s metadata layer introduces overhead or delivers optimization for quantitative research use cases, comparing it with vanilla Parquet access on Amazon S3. We examine how these approaches impact common quant research queries and workflows.</p> 
<p>The key question is whether Iceberg’s metadata layer, designed to optimize vanilla Parquet access on Amazon S3, introduces overhead or delivers the intended optimization for quantitative research use cases. Then we discuss overlapping optimization techniques, such as data distribution and sorting. We also discuss that there is no magic partitioning and all sorting scheme where one size fits all in the context of quant research. Our benchmarks show that Iceberg performs comparably to direct Amazon S3 access, with additional optimizations from its metadata and statistics usage, similar to database indexing.</p> 
<h3>Vanilla Parquet vs Iceberg: Amazon S3 read performance</h3> 
<p>We created four different datasets: two using Iceberg and two with direct Amazon S3 Parquet access, each with both sorted and unsorted write distributions. The purpose of this exercise was to compare the performance of direct Amazon S3 Parquet access vs. the Iceberg open table format, taking into account the impact of write distribution patterns when running various queries commonly used in quantitative trading research.</p> 
<h4>Query 1</h4> 
<p>We first run a simple count query to get the total number of records in the table. This query helps understand the baseline performance for a straightforward operation. For example, if the table contains tick-level market data for various financial instruments, the count can give an idea of the total number of data points available for analysis.</p> 
<p>The following is the code for vanilla Parquet:</p> 
<div class="hide-language"> 
 <div class="hide-language"> 
  <pre><code class="lang-sql">count = spark.read.parquet(s3://example-s3-bucket/path/to/data).count()</code></pre> 
 </div> 
 <p>The following is the code for Iceberg:</p> 
</div> 
<div class="hide-language"> 
 <pre><code class="lang-sql">count = spark.read.table(table_name).count()
# We used typical count query for the performance comparision however this could have been also done using metadata as shown below which completes in few seconds 
spark.read.format("iceberg").load(f"{table_name}.files").select(sum("record_count")).show(truncate=False)</code></pre> 
</div> 
<h4>Query 2</h4> 
<p>Our second query is a grouping and counting query to find the number of records for each combination of <code>exchange_code</code> and <code>instrument</code>. This query is commonly used in quantitative trading research to analyze market liquidity and trading activity across different instruments and exchanges.</p> 
<p>The following is the code for vanilla Parquet:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">spark.read.parquet(s3://example-s3-bucket/path/to/data) \
         .groupBy("exchange_code", "instrument") \
         .count() \
         .orderBy("count", ascending=False) \
         .count().show(truncate=False)</code></pre> 
</div> 
<p>The following is the code for Iceberg:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">spark.read.table(table_name) \
        .groupBy("exchange_code", "instrument") \
        .count() \
        .orderBy("count", ascending=False) \
        .show(truncate=False)</code></pre> 
</div> 
<h4>Query 3</h4> 
<p>Next, we run a distinct query to retrieve the distinct combinations of year, month, and day from the <code>adapterTimestamp_ts_utc</code> column. In quantitative trading research, this query can be helpful for understanding the time range covered by the dataset. Researchers can use this information to identify periods of interest for their analysis, such as specific market events, economic cycles, or seasonal patterns.</p> 
<p><strong>&nbsp;</strong>The following is the code for vanilla Parquet:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">spark.read.parquet(s3://example-s3-bucket/path/to/data) \
         .select(f.year("adapterTimestamp_ts_utc").alias("year"),
                 f.month("adapterTimestamp_ts_utc").alias("month"),
                 f.dayofmonth("adapterTimestamp_ts_utc").alias("day")) \
         .distinct() \
         .count() \
         .show(truncate=False)</code></pre> 
</div> 
<p>The following is the code for Iceberg:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">spark.read.table(table_name) \
        .select(f.year("adapterTimestamp_ts_utc").alias("year"),
                f.month("adapterTimestamp_ts_utc").alias("month"),
                f.dayofmonth("adapterTimestamp_ts_utc").alias("day")) \
        .distinct() \
        .count() \
        .show(truncate=False)</code></pre> 
</div> 
<h4>Query 4</h4> 
<p>Lastly, we run a grouping and counting query with a date range filter on the <code>adapterTimestamp_ts_utc</code> column. This query is similar to Query 2 but focuses on a specific time period. You could use this query to analyze market activity or liquidity during specific time periods, such as periods of high volatility, market crashes, or economic events. Researchers can use this information to identify potential trading opportunities or investigate the impact of these events on market dynamics.</p> 
<p><strong>&nbsp;</strong>The following is the code for vanilla Parquet:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">spark.read.parquet(s3://example-s3-bucket/path/to/data) \
         .filter((f.col("adapterTimestamp_ts_utc") &gt;= "2023-04-17 00:00:00") &amp;
                 (f.col("adapterTimestamp_ts_utc") &lt;= "2023-04-18 23:59:59.999")) \
         .groupBy("exchange_code", "instrument") \
         .count() \
         .orderBy("count", ascending=False) \
         .show(truncate=False)</code></pre> 
</div> 
<p>The following is the code for Iceberg. Because Iceberg has a metadata layer, the row count can be fetched from metadata:</p> 
<div class="hide-language"> 
 <pre><code class="lang-sql">spark.read.table(table_name) \
        .filter((f.col("adapterTimestamp_ts_utc") &gt;= "2023-04-17 00:00:00") &amp;
                (f.col("adapterTimestamp_ts_utc") &lt;= "2023-04-18 23:59:59.999")) \
        .groupBy("exchange_code", "instrument") \
        .count() \
        .orderBy("count", ascending=False) \
        .show(truncate=False)</code></pre> 
</div> 
<h3>Test results</h3> 
<p>To evaluate the performance and cost benefits of using Iceberg for our quant research data lake, we created four different datasets: two with Iceberg tables and two with direct Amazon S3 Parquet access, each using both sorted and unsorted write distributions. We first ran <a href="https://aws.amazon.com/glue" rel="noopener" target="_blank">AWS Glue</a> write jobs to create the Iceberg tables and then mirrored the same write processes for the Amazon S3 Parquet datasets. For the unsorted datasets, we partitioned the data by <code>exchange</code> and <code>instrument</code>, and for the sorted datasets, we added a sort key on the time column.</p> 
<p>Next, we ran a series of queries commonly used in quantitative trading research, including simple count queries, grouping and counting, distinct value queries, and queries with date range filters. Our benchmarking process involved reading data from Amazon S3, performing various transformations and joins, and writing the processed data back to Amazon S3 as Parquet files.</p> 
<p>By comparing runtimes and costs across different data formats and write distributions, we quantified the benefits of Iceberg’s optimized data organization, metadata management, and efficient Amazon S3 data handling. The results showed that Iceberg not only enhanced query performance without introducing significant overhead, but also reduced the likelihood of task failures, reruns, and throttling issues, leading to more stable and predictable job execution, particularly with large datasets stored in Amazon S3.</p> 
<h4>AWS Glue write jobs</h4> 
<p>In the following table, we compare the performance and the cost implications of using Iceberg vs. vanilla Parquet access on Amazon S3, taking into account the following use cases:</p> 
<ul> 
 <li>Iceberg table (unsorted) – We created an Iceberg table partitioned by <code>exchange_code</code> and <code>instrument</code> This means that the data was physically partitioned in Amazon S3 based on the unique combinations of <code>exchange_code</code> and <code>instrument</code> values. Partitioning the data in this way can improve query performance, because Iceberg can prune out partitions that aren’t relevant to a particular query, reducing the amount of data that needs to be scanned. The data was not sorted on any column in this case, which is the default behavior.</li> 
 <li>Vanilla Parquet (unsorted) – For this use case, we wrote the data directly as Parquet files to Amazon S3, without using Iceberg. We repartitioned the data by <code>exchange_code</code> and <code>instrument</code> columns using standard hash partitioning before writing it out. Repartitioning was necessary to avoid potential throttling issues when reading the data later, because accessing data directly from Amazon S3 without intelligent partitioning can lead to too many requests hitting the same S3 prefix. Like the Iceberg table, the data was not sorted on any column in this case. To make comparison fair, we used the exact repartition count that Iceberg uses.</li> 
 <li>Iceberg table (sorted) – We created another Iceberg table, this time partitioned by <code>exchange_code</code> and <code>instrument</code> Additionally, we sorted the data in this table on the <code>adapterTimestamp_ts_utc</code> column. Sorting the data can improve query performance for certain types of queries, such as those that involve range filters or ordered outputs. Iceberg automatically handles the sorting and partitioning of the data transparently to the user.</li> 
 <li>Vanilla Parquet (sorted) – For this use case, we again wrote the data directly as Parquet files to Amazon S3, without using Iceberg. We repartitioned the data by range on the <code>exchange_code</code>, <code>instrument</code>, and <code>adapterTimestamp_ts_utc</code> columns before writing it out using standard range partitioning with 1996 partition count, because this was what Iceberg was using based on SparkUI. Repartitioning on the time column (<code>adapterTimestamp_ts_utc</code>) was necessary to achieve a sorted write distribution, because Parquet files are sorted within each partition. This sorted write distribution can improve query performance for certain types of queries, similar to the sorted Iceberg table.</li> 
</ul> 
<table border="1px" cellpadding="10px" width="100%"> 
 <tbody> 
  <tr style="background-color: #000000;"> 
   <td width="23%"><span style="color: #ffffff;"><strong>Write Distribution Pattern</strong></span></td> 
   <td width="21%"><span style="color: #ffffff;"><strong>Iceberg Table (Unsorted)</strong></span></td> 
   <td width="22%"><span style="color: #ffffff;"><strong>Vanilla Parquet (Unsorted)</strong></span></td> 
   <td width="18%"><span style="color: #ffffff;"><strong>Iceberg Table (Sorted)</strong></span></td> 
   <td width="14%"><span style="color: #ffffff;"><strong>Vanilla Parquet<br /> (Sorted)</strong></span></td> 
  </tr> 
  <tr> 
   <td width="23%"><strong>DPU Hours</strong></td> 
   <td width="21%">899.46639</td> 
   <td width="22%">915.70222</td> 
   <td width="18%">1402</td> 
   <td width="14%">1365</td> 
  </tr> 
  <tr> 
   <td width="23%"><strong>Number of S3 Objects</strong></td> 
   <td width="21%">7444</td> 
   <td width="22%">7288</td> 
   <td width="18%">9283</td> 
   <td width="14%">9283</td> 
  </tr> 
  <tr> 
   <td width="23%"><strong>Size of S3 Parquet Objects</strong></td> 
   <td width="21%">567.7 GB</td> 
   <td width="22%">629.8 GB</td> 
   <td width="18%">525.6 GB</td> 
   <td width="14%">627.1 GB</td> 
  </tr> 
  <tr> 
   <td width="23%"><strong>Runtime</strong></td> 
   <td width="21%">1h 51m 40s</td> 
   <td width="22%">1h 53m 29s</td> 
   <td width="18%">2h 52m 7s</td> 
   <td width="14%">2h 47m 36s</td> 
  </tr> 
 </tbody> 
</table> 
<h4>AWS Glue read jobs</h4> 
<p>For the AWS Glue read jobs, we ran a series of queries commonly used in quantitative trading research, such as simple counts, grouping and counting, distinct value queries, and queries with date range filters. We compared the performance of these queries between the Iceberg tables and the vanilla Parquet files read in Amazon S3. In the following table, you can see two AWS Glue jobs that show the performance and cost implications of access patterns described earlier.</p> 
<table border="1px" cellpadding="10px"> 
 <tbody> 
  <tr style="background-color: #000000;"> 
   <td width="331"><span style="color: #ffffff;"><strong>Read Queries / Runtime in Seconds</strong></span></td> 
   <td width="146"><span style="color: #ffffff;"><strong>Iceberg Table</strong></span></td> 
   <td width="146"><span style="color: #ffffff;"><strong>Vanilla Parquet</strong></span></td> 
  </tr> 
  <tr> 
   <td width="331"><strong>COUNT(1) on unsorted</strong></td> 
   <td width="146">35.76s</td> 
   <td width="146">74.62s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>GROUP BY and ORDER BY on unsorted</strong></td> 
   <td width="146">34.29s</td> 
   <td width="146">67.99s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>DISTINCT and SELECT on unsorted</strong></td> 
   <td width="146">51.40s</td> 
   <td width="146">82.95s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>FILTER and GROUP BY and ORDER BY on unsorted</strong></td> 
   <td width="146">25.84s</td> 
   <td width="146">49.05s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>COUNT(1) on sorted</strong></td> 
   <td width="146">15.29s</td> 
   <td width="146">24.25s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>GROUP BY and ORDER BY on sorted</strong></td> 
   <td width="146">15.88s</td> 
   <td width="146">28.73s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>DISTINCT and SELECT on sorted</strong></td> 
   <td width="146">30.85s</td> 
   <td width="146">42.06s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>FILTER and GROUP BY and ORDER BY on sorted</strong></td> 
   <td width="146">15.51s</td> 
   <td width="146">31.51s</td> 
  </tr> 
  <tr> 
   <td width="331"><strong>AWS Glue DPU hours</strong></td> 
   <td width="146">45.98</td> 
   <td width="146">67.97</td> 
  </tr> 
 </tbody> 
</table> 
<h4>Test results insights</h4> 
<p>These test results offered the following insights:</p> 
<ul> 
 <li><strong>Accelerated query performance</strong> – Iceberg improved read operations by up to 52% for unsorted data and 51% for sorted data. This speed boost enables quant researchers to analyze larger datasets and test trading strategies more rapidly. In quantitative finance, where speed is crucial, this performance gain allows teams to uncover market insights faster, potentially gaining a competitive edge.</li> 
 <li><strong>Reduced operational costs</strong> – For read-intensive workloads, Iceberg reduced DPU hours by 32.4% and achieved a 10–16% reduction in Amazon S3 storage. These efficiency gains translate to cost savings in data-intensive quant operations. With Iceberg, firms can run more comprehensive analyses within the same budget or reallocate resources to other high-value activities, optimizing their research capabilities.</li> 
 <li><strong>Enhanced data management and scalability</strong> – Iceberg showed comparable write performance for unsorted data (899.47 DPU hours vs. 915.70 for vanilla Parquet) and maintained consistent object counts across sorted and unsorted scenarios (7,444 and 9,283, respectively). This consistency leads to more reliable and predictable job execution. For quant teams dealing with large-scale datasets, this reduces time spent on troubleshooting data infrastructure issues and increases focus on developing trading strategies.</li> 
 <li><strong>Improved productivity</strong> – Iceberg outperformed vanilla Parquet access across various query types. Simple counts were 52.1% faster, grouping and ordering operations improved by 49.6%, and filtered queries were 47.3% faster for unsorted data. This performance enhancement boosts productivity in quant research workflows. It reduces query completion times, allowing quant developers and researchers to spend more time on model development and market analysis, leading to faster iteration on trading strategies.</li> 
</ul> 
<h2>Conclusion</h2> 
<p>Quant research platforms often avoid adopting new data management solutions like Iceberg, fearing performance penalties and increased costs. Our analysis disproves these concerns, demonstrating that Iceberg not only matches or enhances performance compared to direct Amazon S3 access, but also provides substantial additional benefits.</p> 
<p>Our tests reveal that Iceberg significantly accelerates query performance, with improvements of up to 52% for unsorted data and 51% for sorted data. This speed boost enables quant researchers to analyze larger datasets and test trading strategies more rapidly, potentially uncovering valuable market insights faster.</p> 
<p>Iceberg streamlines data management tasks, allowing researchers to focus on strategy development. Its robust insert, update, and delete capabilities, combined with time travel features, enable effortless management of complex datasets, improving backtest accuracy and facilitating rapid strategy iteration.</p> 
<p>The platform’s intelligent handling of partitioning and Amazon S3 API quota issues eliminates undifferentiated heavy lifting, freeing quant teams from low-level data engineering tasks. This automation redirects efforts to high-value activities such as model development and market analysis. Moreover, our tests show that for read-intensive workloads, Iceberg reduced DPU hours by 32.4% and achieved a 10–16% reduction in Amazon S3 storage, leading to significant cost savings.</p> 
<p>Flexibility is a key advantage of Iceberg. Its various interfaces, including SQL, DataFrames, and programmatic APIs, integrate seamlessly with existing quant research workflows, accommodating diverse analysis needs and coding preferences.</p> 
<p>By adopting Iceberg, quant research teams gain both performance enhancements and powerful data management tools. This combination creates an environment where researchers can push analytical boundaries, maintain high data integrity standards, and focus on generating valuable insights. The improved productivity and reduced operational costs enable quant teams to allocate resources more effectively, ultimately leading to a more competitive edge in quantitative finance.</p> 
<hr /> 
<h3>About the Authors</h3> 
<p style="clear: both;"><strong><img alt="" class="size-full wp-image-49817 alignleft" height="150" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2023/06/24/Guy_bio_100px.png" width="100" />Guy Bachar</strong> is a Senior Solutions Architect at AWS based in New York. He specializes in assisting capital markets customers with their cloud transformation journeys. His expertise encompasses identity management, security, and unified communication.</p> 
<p style="clear: both;"><img alt="Sercan Karaoglu" class="alignleft" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/01/06/sercanka-100px.jpg" width="125" /><strong>Sercan Karaoglu</strong> is Senior Solutions Architect, specialized in capital markets. He is a former data engineer and passionate about quantitative investment research.</p> 
<p style="clear: both;"><img alt="Boris Litvin" class="alignleft" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/01/06/blitvin-100px.jpg" width="125" /><strong>Boris Litvin</strong> is a Principal Solutions Architect at AWS. His job is in financial services industry innovation. Boris joined AWS from the industry, most recently Goldman Sachs, where he held a variety of quantitative roles across equity, FX, and interest rates, and was CEO and Founder of a quantitative trading FinTech startup.</p> 
<p style="clear: both;"><img alt="Salim Tutuncu" class="alignleft" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/01/06/tutuncus-100px.jpg" width="125" /><strong>Salim Tutuncu</strong> is a Senior Partner Solutions Architect Specialist on Data &amp; AI, based in Dubai with a focus on the EMEA. With a background in the technology sector that spans roles as a data engineer, data scientist, and machine learning engineer, Salim has built a formidable expertise in navigating the complex landscape of data and artificial intelligence. His current role involves working closely with partners to develop long-term, profitable businesses using the AWS platform, particularly in data and AI use cases.</p> 
<p style="clear: both;"><img alt="Alex Tarasov" class="alignleft" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/01/06/alexvt-100px.jpg" width="125" /><strong>Alex Tarasov</strong> is a Senior Solutions Architect working with Fintech startup customers, helping them to design and run their data workloads on AWS. He is a former data engineer and is passionate about all things data and machine learning.</p> 
<p style="clear: both;"><img alt="Jiwan Panjiker" class="alignleft" src="https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/01/06/jpanjike-100px.jpg" width="125" /><strong>Jiwan Panjiker</strong> is a Solutions Architect at Amazon Web Services, based in the Greater New York City area. He works with AWS enterprise customers, helping them in their cloud journey to solve complex business problems by making effective use of AWS services. Outside of work, he likes spending time with his friends and family, going for long drives, and exploring local cuisine.</p>
