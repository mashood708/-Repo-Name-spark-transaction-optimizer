# -Repo-Name-spark-transaction-optimizer
“End-to-end Spark optimization project processing 10 million synthetic transactions with 16 performance techniques, including pushdowns, broadcast joins, AQE, and partition tuning.”

# Spark Transaction Optimizer 🚀

This project demonstrates a production-style Spark optimization pipeline handling:

- **10 million synthetic transactions**
- **5 million synthetic customers**
- **10,000 synthetic products**

It applies **16 critical Spark performance techniques** to show measurable improvements, including:

✅ Column pruning  
✅ Predicate and filter pushdown  
✅ Project pushdown  
✅ Sorting for join keys  
✅ Efficient file formats (Parquet)  
✅ WholeStage code generation  
✅ Adaptive Query Execution (AQE)  
✅ Kryo serialization  
✅ Broadcast joins for small dimension tables  
✅ Avoiding UDFs  
✅ Minimizing data movement with repartitioning  
✅ Tuning shuffle partitions  
✅ Avoiding collect on large datasets  
✅ DataFrame reuse with caching  
✅ Writing data with optimal partition sizing  
✅ Minimizing shuffle

The entire pipeline is fully **reproducible** in Google Colab or Databricks.

---

## 📁 Project Structure

- `01_data_generation.py`: creates the synthetic datasets (customers, products, transactions)  
- `02_baseline.py`: runs the baseline query performance  
- `03_optimizations.py`: applies the 16 optimizations step by step  
- `README.md`: project overview  
- `final_sales_data/`: final optimized Parquet data

---

## 🚀 Key Technologies

- Apache Spark (PySpark)  
- Parquet  
- Google Colab / Databricks  

---

## 📊 Results

- Baseline execution time
- Optimized execution time with column pruning and broadcast joins
- Final partitioned data
- Overall performance improvement documented in the Spark UI

---

## 📌 Usage

```bash
# run from Google Colab
!pip install pyspark
