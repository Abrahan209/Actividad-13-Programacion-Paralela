import subprocess
import time

# Configura rutas de dataset y carpetas de salida
INPUT_PATH = "data/texto.txt"
OUTPUT_RDD = "output/rdd_wordcount"
OUTPUT_DF = "output/df_wordcount"

# Ejecutar Word Count con RDDs
print("Ejecutando Word Count con RDDs...")
start_rdd = time.time()
subprocess.run(["python", "wordcount_rdd.py", INPUT_PATH, OUTPUT_RDD])
rdd_time = time.time() - start_rdd

# Ejecutar Word Count con DataFrames
print("Ejecutando Word Count con DataFrames...")
start_df = time.time()
subprocess.run(["python", "wordcount_df.py", INPUT_PATH, OUTPUT_DF])
df_time = time.time() - start_df

# Comparar tiempos
speedup = rdd_time / df_time
print("\n--- Resultados ---")
print(f"Tiempo RDD: {rdd_time:.2f} segundos")
print(f"Tiempo DataFrame: {df_time:.2f} segundos")
print(f"Speedup (RDD/DataFrame): {speedup:.2f}x más rápido")
