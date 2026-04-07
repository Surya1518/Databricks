# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC ## Parse Customer Details PDF using `ai_parse_document()`
# MAGIC
# MAGIC This notebook demonstrates how to extract **structured tabular data** from an unstructured PDF document using the Databricks SQL AI function `ai_parse_document()`. 
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. **Read** the PDF as a binary file from a Unity Catalog Volume
# MAGIC 2. **Parse** the document using `ai_parse_document()` to extract elements (text, tables, figures, etc.)
# MAGIC 3. **Filter** only the `table` type elements from the parsed output
# MAGIC 4. **Convert** the HTML table content into Spark DataFrames
# MAGIC 5. **Display** each of the 4 data tables individually
# MAGIC
# MAGIC **Source File:** `/Volumes/cdigitpoc/0_bronze/table_config/customer_details_report.pdf`

# COMMAND ----------

# DBTITLE 1,Step 1 Explanation
# MAGIC %md
# MAGIC ### Step 1: Read the PDF as a Binary File
# MAGIC
# MAGIC We use `spark.read.format("binaryFile")` to load the PDF file. This reads the file as raw bytes into a DataFrame with columns:
# MAGIC - `path` — file location
# MAGIC - `modificationTime` — last modified timestamp
# MAGIC - `length` — file size in bytes
# MAGIC - `content` — the raw binary content (used by `ai_parse_document`)

# COMMAND ----------

# DBTITLE 1,Step 2 Explanation
# MAGIC %md
# MAGIC ### Step 2: Parse the Document with `ai_parse_document()`
# MAGIC
# MAGIC The `ai_parse_document(content)` function takes the **binary content** column and returns a **VARIANT** type containing:
# MAGIC - `document.pages` — array of page metadata (dimensions, page numbers)
# MAGIC - `document.elements` — array of extracted elements (text, tables, figures, headers, footers, etc.)
# MAGIC - `metadata` — file information and schema version
# MAGIC - `error_status` — any processing errors
# MAGIC
# MAGIC > **Note:** Since it returns VARIANT, you must use `:` colon notation (not dot `.` notation) for field access.

# COMMAND ----------

# DBTITLE 1,Step 4 Explanation
# MAGIC %md
# MAGIC ### Step 4: Explode Elements and Filter Tables
# MAGIC
# MAGIC This cell performs two key operations:
# MAGIC
# MAGIC 1. **`try_cast(parsed:document:elements AS ARRAY<VARIANT>)`** — Casts the VARIANT elements array into a typed `ARRAY<VARIANT>` so that `explode()` can work on it (explode requires ARRAY or MAP, not raw VARIANT)
# MAGIC 2. **`explode(...).alias("element")`** — Flattens the array so each document element becomes its own row
# MAGIC 3. **`filter(element:type::STRING == "table")`** — Keeps only elements of type `table`, discarding text, headers, figures, etc.
# MAGIC
# MAGIC Each table element contains an `content` field with the table data in **HTML format** (`<table><tr><td>...</td></tr></table>`).

# COMMAND ----------

# DBTITLE 1,Step 5 Explanation
# MAGIC %md
# MAGIC ### Step 5: Parse HTML Tables into Spark DataFrames
# MAGIC
# MAGIC Since `ai_parse_document` returns table data as **HTML strings**, we need a custom parser:
# MAGIC
# MAGIC - **`HTMLTableParser`** — A lightweight Python class (using built-in `html.parser`) that walks through HTML tags:
# MAGIC   - `<th>` / `<td>` → captures cell text
# MAGIC   - `<tr>` → groups cells into rows
# MAGIC - **`html_to_spark_df()`** — Converts parsed rows into a pandas DataFrame (first row = headers), then to a Spark DataFrame
# MAGIC - **`table_html_list[1:]`** — Skips the first table (report metadata) and keeps only the 4 data tables

# COMMAND ----------

# DBTITLE 1,Step 6 Explanation
# MAGIC %md
# MAGIC ### Step 6: Display All 4 Tables
# MAGIC
# MAGIC Loops through the 4 extracted tables and displays each with a labeled header:
# MAGIC
# MAGIC | # | Table Name | Description |
# MAGIC |---|---|---|
# MAGIC | 1 | **Customer Details** | Customer IDs, names, contact info, city, status |
# MAGIC | 2 | **Order History** | Orders with products, amounts, payment methods, delivery status |
# MAGIC | 3 | **Subscription Plans** | Plan tiers, billing cycles, monthly fees |
# MAGIC | 4 | **Support Tickets** | Issue types, priorities, assigned agents, resolution status |

# COMMAND ----------

# DBTITLE 1,Step 1: Read PDF as binary file
df = spark.read.format("binaryFile") \
    .load("/Volumes/cdigitpoc/0_bronze/table_config/customer_details_report.pdf")

# COMMAND ----------

# DBTITLE 1,Step 2: Parse document with ai_parse_document
from pyspark.sql.functions import expr

parsed_df = df.withColumn(
    "parsed",
    expr("ai_parse_document(content)")
)

# COMMAND ----------

# DBTITLE 1,Step 3: Preview parsed output
parsed_df.display()

# COMMAND ----------

# DBTITLE 1,Step 4: Explode elements and filter tables
from pyspark.sql.functions import col, explode, expr

tables_df = parsed_df.select(
    explode(expr("try_cast(parsed:document:elements AS ARRAY<VARIANT>)")).alias("element")
)

tables_only = tables_df.filter(expr("element:type::STRING") == "table")

# COMMAND ----------

# DBTITLE 1,Step 5: Parse HTML tables into Spark DataFrames
from html.parser import HTMLParser
import pandas as pd

class HTMLTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows, self.current_row, self.current_cell = [], [], ""
        self.in_cell = False
    def handle_starttag(self, tag, attrs):
        if tag in ('td', 'th'):
            self.in_cell, self.current_cell = True, ""
    def handle_endtag(self, tag):
        if tag in ('td', 'th'):
            self.in_cell = False
            self.current_row.append(self.current_cell)
        elif tag == 'tr' and self.current_row:
            self.rows.append(self.current_row)
            self.current_row = []
    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

def html_to_spark_df(html):
    parser = HTMLTableParser()
    parser.feed(html)
    headers = parser.rows[0]
    data = parser.rows[1:]
    return spark.createDataFrame(pd.DataFrame(data, columns=headers))

# Collect HTML content from all table elements
table_html_list = tables_only.select(expr("element:content::STRING").alias("html")).collect()

# Skip the first metadata table, keep the 4 data tables
data_tables = [html_to_spark_df(row["html"]) for row in table_html_list[1:]]

# COMMAND ----------

# DBTITLE 1,Step 6: Display all 4 extracted tables
table_names = [
    "Table 1: Customer Details",
    "Table 2: Order History",
    "Table 3: Subscription Plans",
    "Table 4: Support Tickets"
]

for name, df in zip(table_names, data_tables):
    print(f"\n{'='*80}")
    print(f"  {name}")
    print(f"{'='*80}")
    display(df)