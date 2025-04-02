# handson-09-Real-Time Ride-Sharing Analytics with Apache Spark

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Netcat (for Streaming Simulation)**:
   - Install using:
      ```bash
     sudo apt-get install netcat  # Linux
     brew install netcat          # macOS
     ```     

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

- **input/**: Contains the sample input dataset.  
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **README.md**: Assignment instructions and guidelines.

## **Assignment Tasks**  

### **Task-1. Basic Streaming Ingestion and Parsing**

Ingest streaming data from a socket and parse it into a Spark DataFrame.

**Instructions:**

1. **Start the data stream simulation:**:
   ```bash
   Python data_generator.py
   
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
 
     spark-submit src/task1.py

   ```

  **Key Steps in task1_ingestion.py**:

- Create a Spark session.
- Read streaming data from a socket (localhost:9999).
- Parse incoming JSON messages into a DataFrame.
- Print parsed data to the console.

  ## Sample Output:

 ```
+--------+---------+------------+-----------+-------------------+
|trip_id |driver_id|distance_km |fare_amount|timestamp         |
+--------+---------+------------+-----------+-------------------+
|1001    |200      |12.5        |25.0       |2025-04-01 10:00:00|
|1002    |201      |7.8         |15.3       |2025-04-01 10:02:00|
+--------+---------+------------+-----------+-------------------+
```

### **Task-2. Real-Time Aggregations (Driver-Level)**

Compute real-time aggregations on driver earnings and trip distances.

**Instructions:**

1. **Run the aggregation script:**:
   ```bash
      spark-submit src/task2.py
   ```
  **Key Steps in task2_aggregations.py**:

- Reuse the parsed DataFrame from Task 1.
- Group by driver_id and compute:
    - Total earnings: SUM(fare_amount) AS total_fare
    - Average trip distance: AVG(distance_km) AS avg_distance
- Write the output to CSV in real time.

  ## Sample Output:

```
+----------+----------+-------------+
|driver_id |total_fare|avg_distance |
+----------+----------+-------------+
|200       |75.5      |8.6          |
|201       |120.8     |10.2         |
+----------+----------+-------------+
```

### **Task-3. Windowed Time-Based Analytics**

Analyze trends over time using a sliding time window.

**Instructions:**

1. **Run the aggregation script:**:
   ```bash
      spark-submit src/task3.py
   ```
  **Key Steps in task2_aggregations.py**:

- Key Steps in task3_windowed_analysis.py:
- Convert the timestamp column to TimestampType.
- Use Spark’s window function to:
   - Aggregate fare_amount over a 5-minute window, sliding by 1 minute.
- Write the results to a CSV file.

  ## Sample Output:

```
+----------------------+----------------------+-------------+
|window_start          |window_end            |total_fare   |
+----------------------+----------------------+-------------+
|2025-04-01 10:00:00   |2025-04-01 10:05:00   |200.5        |
|2025-04-01 10:01:00   |2025-04-01 10:06:00   |215.3        |
+----------------------+----------------------+-------------+
```

## **Execution Commands**

```
spark-submit src/task1_ingestion.py
spark-submit src/task2_aggregations.py
spark-submit src/task3_windowed_analysis.py
```

## **Follow this commands to push the outputs to the github**:

```
git add .
git commit -m"commit message"
git push origin master
```
