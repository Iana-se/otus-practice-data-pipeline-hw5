"""
Script: pyspark_script.py
Description: PySpark script for testing.
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, col

accesskey = "accesskey"
secretkey = "secretkey"
data_path = "s3a://data_path/"

# Убираем выбросы (IQR)
def remove_outliers_iqr(df, columns):
    for col_name in columns:
        q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df.filter((col(col_name) >= lower_bound) & (col(col_name) <= upper_bound))
    return df

def process_fraud_data(source_path: str, output_path: str) -> None:
    """
    Reads TXT files from a source S3 bucket, cleans and validates the transaction data,
    and saves the result as a Parquet file to the specified output S3 bucket.

    The processing steps include:
    1. Reading TXT files with a predefined schema.
       Assumes TXT files are structured like CSV (e.g., comma- or tab-separated).
    2. Dropping duplicate transactions based on 'transaction_id'.
    3. Filtering out rows with negative values in numeric columns.
    4. Converting 'tx_datetime' to timestamp and removing invalid timestamps.
    5. (Optional) Removing outliers from numeric columns using IQR method.
    6. Ordering the data by 'transaction_id'.
    7. Writing the cleaned DataFrame as a Parquet file to the output path.

    Parameters
    ----------
    source_path : str
        Path to the source S3 bucket or local directory containing TXT files.
    output_path : str
        Path to the output S3 bucket or local directory to save the resulting Parquet file.
    """

    # spark = SparkSession.builder \
    # .appName("TransactionValidation") \
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # .config("spark.hadoop.fs.s3a.access.key", accesskey) \
    # .config("spark.hadoop.fs.s3a.secret.key", secretkey) \
    # .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
    # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    # .config("spark.sql.repl.eagerEval.enabled", True) \
    # .getOrCreate()

    spark = (
    SparkSession.builder
    .appName("TransactionValidation")
    # тюнинг Spark под ноды (s3-c4-m16)
    .config("spark.executor.instances", "3")   # по одному экзекутору на каждую ноду
    .config("spark.executor.cores", "4")       # используем все CPU
    .config("spark.executor.memory", "12g")    # оставляем запас на OS + YARN (из 16 ГБ)
    .config("spark.sql.shuffle.partitions", "300")  # меньше шардов → меньше shuffle
    .config("spark.memory.fraction", "0.6")    # больше памяти под dataframes, меньше под shuffle spill
    # S3 доступ
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", accesskey)
    .config("spark.hadoop.fs.s3a.secret.key", secretkey)
    .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
    
    df = spark.read.option("comment", "#").option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .schema("transaction_id LONG, tx_datetime STRING, customer_id INT, terminal_id INT, tx_amount DOUBLE, tx_time_seconds LONG, tx_time_days LONG, tx_fraud INT, tx_fraud_scenario INT") \
    .csv(source_path)

    # Сначала ОБЯЗАТЕЛЬНО уменьшаем партиции
    # df_optimized = df.coalesce(100)  # уменьшаем до 100
    df_optimized = df.repartition(300)

    df_clean = df_optimized.dropDuplicates(['transaction_id'])

    # Валидация: все числовые поля должны быть >= 0
    df_clean = df_clean.filter(
        (col("transaction_id") >= 0) &
        (col("customer_id") >= 0) &
        (col("terminal_id") >= 0) &
        (col("tx_amount") >= 0) &
        (col("tx_time_seconds") >= 0) &
        (col("tx_time_days") >= 0) &
        (col("tx_fraud") >= 0) &
        (col("tx_fraud_scenario") >= 0)
    )

    # Преобразуем колонку в тип timestamp и одновременно отфильтруем некорректные значения
    df_clean = df_clean.withColumn("tx_datetime", to_timestamp("tx_datetime", "yyyy-MM-dd HH:mm:ss")) \
        .filter(col("tx_datetime").isNotNull())
    
    df_clean = remove_outliers_iqr(df_clean, ["tx_amount", "tx_time_seconds", "tx_time_days"])

    df_clean=df_clean.orderBy("transaction_id")

    df_clean.write.mode("overwrite").option("header", "true").parquet(output_path)

    # spark = (SparkSession
    #     .builder
    #     .appName("sum-csv-files")
    #     .enableHiveSupport()
    #     .getOrCreate()
    # )

    # df = (spark
    #     .read
    #     .option('comment', '#')
    #     .schema("field INT")
    #     .format('csv')
    #     .load(source_path)
    # )

    # sum_value = df.agg(spark_sum('field').alias('sum')).collect()[0]['sum']

    # result_df = spark.createDataFrame(
    #     [(sum_value,)],
    #     ["result"]
    # )

    # result_df.write.mode("overwrite").parquet(output_path)
    print("Successfully saved the result to the output bucket!")


def main():
    """Main function to execute the PySpark job"""
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set")

    input_path = f"s3a://{bucket_name}/input_data/*.txt"
    output_path = f"s3a://{bucket_name}/output_data/processed_fraud_data.parquet"
    process_fraud_data(input_path, output_path)

if __name__ == "__main__":
    main()
