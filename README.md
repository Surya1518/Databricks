# Databricks
This repositary containing the files which I tested in databricks 

# 1. ai_parse_document notebook.py

##  What I Built

In this project, I built a pipeline to extract structured data from an unstructured PDF using Databricks.

The input PDF contains multiple tables such as customer details, purchase history, subscriptions, and support tickets.

---

###  What I Did

- Loaded the PDF from Unity Catalog as a binary file  
- Used `ai_parse_document()` to parse the document  
- Extracted elements from the VARIANT output  
- Filtered only table data  
- Converted HTML tables into Spark DataFrames  
- Created structured, queryable datasets  

---

###  Output

The PDF was successfully transformed into multiple structured tables:

- Customer Details  
- Purchase History  
- Subscription Plans  
- Support Tickets  

---

###  Key Insight

> PDF → Structured Data using AI  

This project demonstrates how unstructured documents can be converted into analytics-ready data using Databricks.

