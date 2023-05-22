-- Databricks notebook source
-- DBTITLE 1,Create Streaming Live Table Bronze
CREATE STREAMING LIVE TABLE streamTabNewsBronze AS
  SELECT
    *
  FROM
    cloud_files("/mnt/tabnewsdata/rawdata", "parquet", map("schema", "id string,
owner_id STRING,
parent_id STRING,
slug STRING,
title STRING,
status STRING,
source_url STRING,
created_at TIMESTAMP,
updated_at TIMESTAMP,
published_at TIMESTAMP,
deleted_at TIMESTAMP,
owner_username STRING,
tabcoins INTEGER,
children_deep_count INTEGER"))

-- COMMAND ----------

-- DBTITLE 1,Create Streaming Live Table Silver
CREATE STREAMING LIVE TABLE streamTabNewsSilver AS
  SELECT
      DISTINCT(id)
    , owner_id  
    , parent_id
    , slug
    , title
    , status
    , source_url
    , created_at
    , updated_at
    , published_at
    , deleted_at
    , owner_username
    , tabcoins
    , children_deep_count
  FROM
    STREAM(LIVE.streamTabNewsBronze)

-- COMMAND ----------

-- DBTITLE 1,Create Streaming Live Table Gold
CREATE LIVE TABLE streamPostsQuatity AS
  SELECT
      owner_id
    , owner_username
    , COUNT(*) AS postsQuantity
  FROM
    LIVE.streamTabNewsSilver
  GROUP BY
      owner_id
    , owner_username
  ORDER BY
    postsQuantity DESC

-- COMMAND ----------

-- DBTITLE 1,Create Bronze Live Table
CREATE LIVE TABLE tabNewsBronze AS
  SELECT
    *
  FROM
    rawtabnewstable

-- COMMAND ----------

-- DBTITLE 1,Create Silver Live Table
CREATE LIVE TABLE tabNewsSilver AS
  SELECT
      DISTINCT(id)
    , owner_id  
    , parent_id
    , slug
    , title
    , status
    , source_url
    , created_at
    , updated_at
    , published_at
    , deleted_at
    , owner_username
    , tabcoins
    , children_deep_count
  FROM
    LIVE.tabNewsBronze

-- COMMAND ----------

-- DBTITLE 1,Create Gold Live Table
CREATE LIVE TABLE postsQuatity AS
  SELECT
      owner_id
    , owner_username
    , COUNT(*) AS postsQuantity
  FROM
    LIVE.tabNewsSilver
  GROUP BY
      owner_id
    , owner_username
  ORDER BY
    postsQuantity DESC
