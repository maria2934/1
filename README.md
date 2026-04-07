# Отчет по лабораторной работе №2: Big Data — Hadoop + Apache Spark

## Датасет

### Основная информация

**Файл:** `crypto_dataset.csv`  
**Скрипт:** `generate_data.py`  
**Строк:** 100 000 
**Столбцов:** 14  
**Размер:** 10,9 MB  
**Период:** 01.01.2025 — 31.12.2025  
**Содержание:** Рыночные и технические показатели криптовалют на различных биржах и в разных регионах мира

### Описание признаков

| Признак | Тип | Описание |
|---|---|---|
| `record_id` | int | Уникальный идентификатор записи |
| `timestamp` | datetime | Время фиксации данных |
| `crypto_type` | str | Тип криптовалюты |
| `exchange` | str | Биржа, на которой произведена сделка |
| `region` | str | Географический регион |
| `price_usd` | float | Цена криптовалюты в USD |
| `volume_usd` | float | Объём торгов в USD |
| `hash_rate_ths` | float | Хешрейт (в TH/s) |
| `blockchain_fees_usd` | float | Комиссия в сети блокчейна (в USD) |
| `is_anomaly` | bool | Флаг аномального значения |
| `energy_consumption_kw` | float | Энергопотребление майнинга (в кВт) |
| `founded_year` | int | Год основания биржи |
| `hq` | str | Страна штаб-квартиры биржи |
| `regulated` | bool | Является ли биржа регулируемой |

### Генерация данных

- Криптовалюты Bitcoin, Ethereum, Solana, Cardano, Polkadot выбираются случайно      
- Биржи Binance, Coinbase, Kraken, FTX, KuCoin, Bybit — с привязкой к году основания, штаб-квартире и статусу регулирования     
- Регионы: North America, Europe, Asia, South America, Africa, Oceania     
- Цены гнерируются на основе базовых значений с добавлением случайного отклонения     
- Временные метки равномерно распределены по всему 2025 году     
- Случайные величины (объём, хешрейт, комиссии, энергопотребление) генерируются в реалистичных диапазонах     
- Seed зафиксирован (random.seed(42)) для воспроизводимости результатов

### Проверка соответствия требованиям

| Критерий | Соответствие |
|---|---|
| 100 000+ строк | Сгенерировано ровно 100 000 строк |
| 6+ признаков | 14 признаков — более чем достаточно | 
| Минимум 3 типа данных | Использованы int, float, bool, str, datetime — всего 5 типов | 
| Хотя бы один категориальный признак | crypto_type, exchange, region, hq — все категориальные | 

### Вывод

Данные структурированы, логичны и пригодны для последующей обработки в экосистеме Hadoop и Spark.

## Развёртывание Hadoop

### Этапы (кратко)

1. Скачали Hadoop 3.3.6

<img width="1238" height="140" alt="image" src="https://github.com/user-attachments/assets/71245945-c6ab-4a8d-8ba6-f95415de4bbd" />

3. Создали симлинки
   
mkdir -p ~/.sdkman/candidates/hadoop    
ln -sfn ~/.hadoop ~/.sdkman/candidates/hadoop/3.3.6    
ln -sfn ~/.hadoop ~/.sdkman/candidates/hadoop/current    

4. Установили Hadoop

<img width="1126" height="278" alt="image" src="https://github.com/user-attachments/assets/3598e745-05b7-4f70-9b1f-82a2a698da05" />

6. Экспортировали переменные
     
export HADOOP_HOME=/Users/mariia/.sdkman/candidates/hadoop/current   
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop  
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin   

7. Настроили SSH

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa   
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys   
chmod 600 ~/.ssh/authorized_keys   

8. Создали конфигурации

hdfs-site.xml
```bash
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!-- Размер блока: 64 МБ -->
  <property>
    <name>dfs.blocksize</name>
    <value>67108864</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///Users/mariia/Desktop/lab2-spark-hadoop/hadoop/storage/namenode</value>
  </property>

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///Users/mariia/Desktop/lab2-spark-hadoop/hadoop/storage/datanode</value>
  </property>

  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

core-site.xml
```bash
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

yarn-site.xml
```bash
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>

  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
  </property>

  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>2048</value> <!-- 2 ГБ RAM -->
  </property>
</configuration>
```

8. Запустили

<img width="624" height="276" alt="image" src="https://github.com/user-attachments/assets/782e1fa4-1405-4e25-ab84-49b8584155a4" />

9. Загрузили crypto_dataset.csv в HDFS

<img width="1126" height="52" alt="image" src="https://github.com/user-attachments/assets/92e527aa-31f6-4b12-9c51-68103d1069c7" />

10. Проверили, что HDFS работает стабильно

<img width="930" height="1126" alt="image" src="https://github.com/user-attachments/assets/584a9dfa-992e-4f05-a0a4-f4d4628990b3" />

### Результаты

- Успешно развернут Hadoop-кластер с одним NameNode и одним DataNode
- Данные из файла crypto_dataset.csv загружены в HDFS в директорию /input
- Установлен пользовательский размер блока — 64 МБ
- Ограничено потребление памяти — 512 МБ на процесс

### Вывод

Кластер Hadoop полностью функционирует и готов к выполнению задач обработки данных с использованием MapReduce, Spark или других фреймворков. 

## Первый эксперимент (1 DataNode, Spark)

### Spark-приложение

app.py
```bash
import os
import time
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, max, min, desc, round

try:
    import psutil
    PSUTIL_AVAILABLE = True
    
    process = psutil.Process()
    peak_memory_mb = 0.0  

    def get_memory_mb():
        """Возвращает текущую RAM в MB"""
        ram = process.memory_info().rss / 1024 / 1024
        global peak_memory_mb
        if ram > peak_memory_mb:
            peak_memory_mb = ram
        return ram

    def log_memory(stage_name):
        """Логирует RAM на этапе и обновляет пик"""
        current = get_memory_mb()
        print(f"[{stage_name}] RAM: {current:.1f} MB (пик: {peak_memory_mb:.1f} MB)")

except ImportError:
    PSUTIL_AVAILABLE = False
    peak_memory_mb = "N/A"

    def get_memory_mb():
        return "N/A"

    def log_memory(stage_name):
        print(f"[{stage_name}] RAM: недоступно (psutil не установлен)")

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.showConsoleProgress=false pyspark-shell'

spark = SparkSession.builder \
    .appName("CryptoAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.ui.enabled", "true") \
    .config("spark.sql.ui.retainedExecutions", "5") \
    .config("spark.ui.retainedJobs", "5") \
    .config("spark.ui.retainedStages", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_counter = 0

def increment_job():
    global job_counter
    job_counter += 1
    current_ram = get_memory_mb()
    ram_str = f"{current_ram:.1f} MB" if isinstance(current_ram, float) else current_ram
    print(f"Job {job_counter}: Запуск операции | RAM: {ram_str}")

print("Начало выполнения Spark-приложения:", time.strftime("%Y-%m-%d %H:%M:%S"))

start_ram = get_memory_mb()
start_time = time.time()
log_memory("Старт приложения")

print("Чтение данных из HDFS: hdfs://localhost:9000/input/crypto_dataset.csv")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/input/crypto_dataset.csv")

row_count = df.count()
log_memory("После df.count()")
print(f"Успешно прочитано {row_count} строк")
print("Схема данных:")
df.printSchema()

print("Анализ 1: Средняя цена (USD) по типу криптовалюты")
increment_job()
avg_price = df.filter(col("price_usd").isNotNull()) \
    .groupBy("crypto_type") \
    .agg(round(avg("price_usd"), 2).alias("avg_price_usd")) \
    .orderBy(desc("avg_price_usd"))
avg_price.show()
log_memory("После первого show()")

print("Анализ 2: Количество аномальных записей (is_anomaly == true)")
anomalies = df.filter(col("is_anomaly") == True).count()
log_memory("После count() аномалий")
normal = df.filter(col("is_anomaly") == False).count()
print(f"Аномальные записи: {anomalies}")
print(f"Нормальные записи: {normal}")

print("Анализ 3: Количество записей по регионам")
increment_job()
region_stats = df.groupBy("region") \
    .agg(count("*").alias("record_count")) \
    .orderBy(desc("record_count"))
region_stats.show(truncate=False)
log_memory("После show() region_stats")

print("Анализ 4: Энергопотребление и комиссии (средние значения)")
increment_job()
energy_fees = df.agg(
    round(avg("energy_consumption_kw"), 2).alias("avg_energy_kw"),
    round(avg("blockchain_fees_usd"), 2).alias("avg_fees_usd"),
    round(sum("blockchain_fees_usd"), 2).alias("total_fees_usd")
)
energy_fees.show()
log_memory("После show() energy_fees")

print("Анализ 5: Статистика по объёму торгов (volume_usd)")
increment_job()
volume_stats = df.agg(
    round(avg("volume_usd"), 2).alias("avg_volume"),
    round(min("volume_usd"), 2).alias("min_volume"),
    round(max("volume_usd"), 2).alias("max_volume")
)
volume_stats.show()
log_memory("После show() volume_stats")

output_path = "hdfs://localhost:9000/output"
print(f"Сохранение результатов в: {output_path}")
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/output"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
increment_job()
avg_price.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/output/avg_price_by_crypto")
log_memory("После записи в HDFS")
print("Результаты сохранены в HDFS")

end_time = time.time()
duration = end_time - start_time
final_ram = get_memory_mb()

report = f"""
==================================================
ФИНАЛЬНЫЙ ОТЧЁТ ПО ВЫПОЛНЕНИЮ
==================================================
Время выполнения: {duration:.2f} секунд
Пиковая RAM: {peak_memory_mb:.1f} MB
Обработано строк: {row_count}
Всего запущено Jobs: {job_counter}
Spark UI доступен по адресу: http://localhost:4040
==================================================
"""

print(report)

with open("report.txt", "w") as f:
    f.write(report.strip())

print("Финальный отчёт сохранён в report.txt")

spark.stop()
print("SparkSession остановлен. Приложение завершено.")
```

Используется SparkSession, где настроены параметры (UI, adaptive query, логи). Данное приложение читает, обрабатывает, сохраняет.

### Замер времени выполнения 

start_time = time.time() и end_time в конце

### Замер RAM (памяти)

Используется psutil - реальное потребление RAM процесса. Выводится пиковое значение.

### Логирование ключевых моментов

Вывод: схема, количество строк, анализы, статистика. Каждый show() и write() сопровождается пояснением. Increment_job() — отслеживает запуск действий






















