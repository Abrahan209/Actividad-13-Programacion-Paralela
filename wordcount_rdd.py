from pyspark.sql import SparkSession
import time
import sys
import shutil
import os

def main(input_path, output_path):
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    # Crear SparkSession y SparkContext
    spark = SparkSession.builder \
        .appName("WordCountRDD") \
        .getOrCreate()
    sc = spark.sparkContext

    # Medir tiempo
    start_time = time.time()

    # Cargar archivo como RDD
    rdd = sc.textFile(input_path)

    # Word Count con RDDs: flatMap → map → reduceByKey
    word_counts_rdd = (rdd.flatMap(lambda line: line.lower().split())
                          .map(lambda word: (word, 1))
                          .reduceByKey(lambda a, b: a + b))

    # Guardar resultado en archivo de texto
    word_counts_rdd.saveAsTextFile(output_path)

    elapsed_time = time.time() - start_time
    print(f"Tiempo RDD: {elapsed_time:.2f} segundos")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python wordcount_rdd.py <ruta_input> <ruta_output>")
        sys.exit(-1)

    main(sys.argv[1], sys.argv[2])
