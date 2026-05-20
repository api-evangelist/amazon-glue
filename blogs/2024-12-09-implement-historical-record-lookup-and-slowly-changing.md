---
title: "Implement historical record lookup and Slowly Changing Dimensions Type-2 using Apache Iceberg"
url: "https://aws.amazon.com/blogs/big-data/implement-historical-record-lookup-and-slowly-changing-dimensions-type-2-using-apache-iceberg/"
date: "Mon, 09 Dec 2024 22:21:41 +0000"
author: "Tomohiro Tanaka"
feed_url: "https://aws.amazon.com/blogs/big-data/tag/aws-glue/feed/"
---
This post will explore how to look up the history of records and tables using Apache Iceberg, focusing on Slowly Changing Dimensions (SCD) Type-2. This method creates new records for each data change while preserving old ones, thus maintaining a full history. By the end, you'll understand how to use Apache Iceberg to manage historical records effectively on a typical CDC architecture.
