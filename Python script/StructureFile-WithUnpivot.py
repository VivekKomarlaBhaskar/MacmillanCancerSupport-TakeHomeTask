# Databricks notebook source
# This notebook processes structures the cancer registrations file

# COMMAND ----------

# DBTITLE 1,Step 1: Data factory parameters
# Environment = dbutils.widgets.get("Environment")
# StorageAccount = dbutils.widgets.get("StorageAccount")
# SchemaName = dbutils.widgets.get("SchemaName")
# Stream = dbutils.widgets.get("Stream")
# FileName = dbutils.widgets.get("FileName")

# ------------------ This code block defines the data factory variables (Hardcoded to test) ------------------#
# Environment as an input paramter
Environment = "Test"
# Take storage account name as an input parameter Global Variable 'StorageAccount'
StorageAccount = "analyticsgen2test"
# Take schema name for the file as an input parameter
SchemaName = "MacmillanCancerSupport_Test"
# Stream as an input paramter
Stream = "CancerRegistrationBySexAndRegion"
# Filename as an input paramter
FileName = "cancer_registrations_by_sex_and_region_2025-02-24.xlsx"

print(StorageAccount)
print(SchemaName)
print(FileName)
print(Stream)
print(Environment)

# COMMAND ----------

# DBTITLE 1,Step 2: Connect to data lake based on the storage account
if StorageAccount == "analyticsgen2":
    scope_1 = "analyticsgen2"
    key_1 = "analyticsgen2"
else:
    scope_1 = "analyticsgen2test"
    key_1 = "analyticsgen2test"

secret_value_1 = dbutils.secrets.get(scope=scope_1, key=key_1)
account_key = secret_value_1

config_key = f"fs.azure.account.key.{StorageAccount}.dfs.core.windows.net"
spark.conf.set(config_key, account_key)

print(
    f"abfss://datafactory@{StorageAccount}.dfs.core.windows.net/{Environment}/Inbound/{SchemaName}/{Stream}/Landing/{FileName}"
)

# COMMAND ----------

# DBTITLE 1,Step 3: Install libraries and environment
import pandas as pd
import io
from openpyxl import load_workbook
from openpyxl.styles import Border, Side
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date

# COMMAND ----------

# DBTITLE 1,Step 4: Transformation
# Define ABFSS path
source_abfss_path = f"abfss://datafactory@{StorageAccount}.dfs.core.windows.net/{Environment}/Inbound/{SchemaName}/{Stream}/Landing/{FileName}"

# Read file into a Spark DataFrame
df_spark = spark.read.format("binaryFile").load(source_abfss_path)

# Extract file content as bytes
file_content = df_spark.collect()[0]["content"]
file_stream = io.BytesIO(file_content)  # Convert to BytesIO for openpyxl


def process_excel(file_stream, sheet_name="Table 4"):
    """Processes the Excel file with minimal steps to clean and structure data."""
    wb = load_workbook(file_stream, data_only=True)
    ws = wb[sheet_name]

    # Step 1: Load data and remove first 5 rows
    data = list(ws.values)[5:]

    # Step 2: Merge header rows (rows 6-9 in original file)
    headers = [
        " ".join(str(cell).strip() for cell in col if cell).strip()
        for col in zip(*data[:4])
    ]
    data_rows = data[4:]  # Remaining data rows

    # Step 3: Remove rows from "Notes:" onward
    cleaned_data = []
    for row in data_rows:
        if any("Notes:" in str(cell) for cell in row if cell):
            break  # Stop processing at "Notes:"
        cleaned_data.append(row)

    # Step 4: Remove blank rows
    cleaned_data = [
        row for row in cleaned_data if any(cell not in [None, ""] for cell in row)
    ]

    # Convert to DataFrame
    df = pd.DataFrame(cleaned_data, columns=headers)

    # Step 5: Drop first column if all values are null
    if df.iloc[:, 0].isna().all():
        df = df.iloc[:, 1:]

    return df


# Process the Excel file
processed_df = process_excel(file_stream)

# Step 6: Unpivot the Data**
id_vars = ["ICD-10 code", "Site description", "Sex"]  # Columns to keep
value_vars = [
    col for col in processed_df.columns if col not in id_vars
]  # Columns to unpivot

unpivoted_df = processed_df.melt(
    id_vars=id_vars, var_name="Region", value_name="registrationTotals"
)

# Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(unpivoted_df)

# Step 7: Extract ReportDate from FileName**
report_date_str = FileName[-15:].replace(
    ".xlsx", ""
)  # Extract rightmost 15 chars & remove ".xlsx"
spark_df = spark_df.withColumn(
    "ReportDate", to_date(lit(report_date_str), "yyyy-MM-dd")
)

# Show result
display(spark_df)

# COMMAND ----------

# DBTITLE 1,Step 5: Write to ADLS Gen2 for further processing
# Write Spark DataFrame to CSV (single file)
spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "dbfs:/tmp/test/summary_csv"
)

# Find the actual CSV file Spark created
import os

temp_files = os.listdir("/dbfs/tmp/test/summary_csv")
csv_file = [f for f in temp_files if f.endswith(".csv")][0]  # Pick the first CSV file

# Move the CSV file to the final location
dbutils.fs.cp(
    f"dbfs:/tmp/test/summary_csv/{csv_file}",
    f"abfss://datafactory@{StorageAccount}.dfs.core.windows.net/{Environment}/Inbound/{SchemaName}/{Stream}/SynapseProcessing/{FileName.replace('.xlsx', '.csv')}",
)

# Clean up temporary directory
dbutils.fs.rm("dbfs:/tmp/test/summary_csv", True)
