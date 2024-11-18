"""
Script: pyspark_script.py
Description: PySpark script for testing.
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


def sum_csv_files(source_path: str, output_path: str) -> None:
    """
    Reads CSV files from source bucket, calculates sum of 'field' column
    and saves result to output bucket as parquet file

    Parameters
    ----------
    source_path : str
        Path to the source bucket with CSV files
    output_path : str
        Path to the output bucket to save the result
    """

    # Инициализация Spark сессии
    spark = (SparkSession
        .builder
        .appName("sum-csv-files")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Чтение всех CSV файлов из бакета
    df = (spark
        .read
        .option('comment', '#')
        .schema("field INT")
        .format('csv')
        .load(source_path)
    )
    # Вычисление суммы
    sum_value = df.agg(spark_sum('field').alias('sum')).collect()[0]['sum']
    # Создание DataFrame с результатом в одной строке
    result_df = spark.createDataFrame(
        [(sum_value,)],
        ["result"]
    )

    # Запись результата в формате parquet
    result_df.write.mode("overwrite").parquet(output_path)

def main():
    """Main function to execute the PySpark job"""

    # Получение имени бакета из аргументов командной строки
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set")

    # Формирование путей для входных и выходных данных
    input_path = f"{bucket_name}/input_data/*.csv"
    output_path = f"{bucket_name}/output_data/sum_data.parquet"
    sum_csv_files(input_path, output_path)

if __name__ == "__main__":
    main()
