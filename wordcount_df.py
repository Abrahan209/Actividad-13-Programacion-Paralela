from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
import time
import sys

def main(input_path, output_path):
    # Crear SparkSession
    spark = SparkSession.builder \
        .appName("WordCountDataFrame") \
        .getOrCreate()

    # Medir tiempo
    start_time = time.time()

    # Cargar archivo como DataFrame
    df = spark.read.text(input_path)

    # Word Count con DataFrames: explode → split → groupBy → count → orderBy
    words_df = (df.select(explode(split(col("value"), r"\s+")).alias("word"))
                  .groupBy("word")
                  .count()
                  .orderBy(col("count").desc()))

    # Guardar resultado en CSV
    words_df.write.csv(output_path, header=True)

    elapsed_time = time.time() - start_time
    print(f"Tiempo DataFrame: {elapsed_time:.2f} segundos")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python wordcount_df.py <ruta_input> <ruta_output>")
        sys.exit(-1)

    main(sys.argv[1], sys.argv[2])
