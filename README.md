# Amazon Glue (amazon-glue)
Amazon Glue is a serverless data integration service that makes it simple to discover, prepare, move, and integrate data from multiple sources for analytics, machine learning, and application development. It provides both visual and code-based interfaces for ETL operations and includes a Data Catalog for unified metadata management.

**URL:** [https://aws.amazon.com/glue/](https://aws.amazon.com/glue/)

**Run:** [Capabilities Using Naftiko](https://github.com/naftiko/fleet?utm_source=api-evangelist&utm_medium=readme&utm_campaign=company-api-evangelist&utm_content=repo)

## Tags:

 - Analytics, AWS, Data Catalog, Data Integration, Data Pipeline, ETL, Serverless

## Timestamps

- **Created:** 2024-01-15
- **Modified:** 2026-04-19

## APIs

### Amazon Glue API
The Amazon Glue API enables programmatic access to create and manage ETL jobs, crawlers, data catalogs, connections, and development endpoints.

**Human URL:** [https://aws.amazon.com/glue/](https://aws.amazon.com/glue/)

#### Tags:

 - Analytics, Data Catalog, Data Integration, ETL

#### Properties

- [Documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html)
- [OpenAPI](openapi/amazon-glue-openapi.yml)
- [GettingStarted](https://aws.amazon.com/glue/getting-started/)
- [Pricing](https://aws.amazon.com/glue/pricing/)
- [FAQ](https://aws.amazon.com/glue/faqs/)
- [APIReference](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html)
- [JSONSchema](json-schema/glue-job-schema.json)
- [JSONLD](json-ld/amazon-glue-context.jsonld)

## Common Properties

- [Portal](https://aws.amazon.com/glue/)
- [Documentation](https://docs.aws.amazon.com/glue/)
- [TermsOfService](https://aws.amazon.com/service-terms/)
- [PrivacyPolicy](https://aws.amazon.com/privacy/)
- [Support](https://aws.amazon.com/premiumsupport/)
- [Blog](https://aws.amazon.com/blogs/big-data/tag/aws-glue/)
- [GitHubOrganization](https://github.com/aws)
- [Console](https://console.aws.amazon.com/glue/)
- [SignUp](https://portal.aws.amazon.com/billing/signup)
- [StatusPage](https://health.aws.amazon.com/health/status)
- [Contact](https://aws.amazon.com/contact-us/)

## Features

| Name | Description |
|------|-------------|
| Serverless ETL | Run ETL jobs without managing infrastructure with automatic scaling and pay-per-use pricing. |
| Visual ETL Editor | Build ETL pipelines visually using drag-and-drop without writing code. |
| Data Catalog | Unified metadata repository for all data assets across S3, databases, and data warehouses. |
| Automated Schema Discovery | Crawlers automatically discover data schemas and populate the Data Catalog. |
| Workflow Orchestration | Orchestrate multi-job ETL pipelines with triggers and conditional flows. |
| ML Transforms | Use machine learning to automate complex data transformation tasks. |
| Schema Registry | Centrally manage and enforce data schema evolution with versioning. |
| Data Quality | Define and evaluate data quality rules to validate data during ETL processing. |

## Use Cases

| Name | Description |
|------|-------------|
| Data Lake ETL | Build ETL pipelines to ingest, transform, and load data into Amazon S3 data lakes. |
| Data Warehouse Loading | Extract and transform data from multiple sources and load into Amazon Redshift. |
| Data Catalog Management | Maintain a unified data catalog for data discovery across all data assets. |
| Real-Time Streaming ETL | Process streaming data from Kinesis and Kafka with Glue Streaming jobs. |
| Machine Learning Data Prep | Prepare and transform training datasets for machine learning using Glue Studio. |

## Integrations

| Name | Description |
|------|-------------|
| Amazon S3 | Primary data lake storage for Glue ETL input and output. |
| Amazon Redshift | Load transformed data into Redshift data warehouse. |
| Amazon Athena | Query Data Catalog tables directly with Athena serverless SQL. |
| Amazon Kinesis | Process streaming data from Kinesis Data Streams with Glue streaming. |
| AWS Lake Formation | Fine-grained access control to Glue Data Catalog resources. |

## Artifacts

### OpenAPI

- [Amazon Glue OpenAPI](openapi/amazon-glue-openapi.yml) — 202 operations

### JSON Schema

500 schema files in [json-schema/](json-schema/)

### JSON Structure

500 structure files in [json-structure/](json-structure/)

### JSON-LD

- [Amazon Glue Context](json-ld/amazon-glue-context.jsonld)

### Examples

500 example files in [examples/](examples/)

## Capabilities

### Shared Per-API Definitions

- [Amazon Glue](capabilities/shared/amazon-glue.yaml) — 12 operations for ETL and data catalog management

### Workflow Capabilities

| Workflow | APIs Combined | Tools | Persona |
|----------|--------------|-------|---------|
| [Amazon Glue Data Integration](capabilities/amazon-glue-data-integration.yaml) | Amazon Glue | 15 | Data Engineer, Data Analyst |

## Vocabulary

- [Amazon Glue Vocabulary](vocabulary/amazon-glue-vocabulary.yaml) — Unified taxonomy mapping 10 resources, 7 actions, 1 workflow, and 2 personas

## Rules

- [Amazon Glue Spectral Rules](rules/amazon-glue-spectral-rules.yml) — 8 rules enforcing Amazon Glue API conventions

## Maintainers

**FN:** Kin Lane

**Email:** kin@apievangelist.com
