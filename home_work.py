from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, desc

ROUND_DIGITS = 2
spark = SparkSession.builder.appName("HomeWork1").getOrCreate()


# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.

products_df = spark.read.csv('products.csv', header=True)
purchases_df = spark.read.csv('purchases.csv', header=True)
users_df = spark.read.csv('users.csv', header=True)

print(f"products_df_count in original Datasets: "
      f"{products_df.count()} purchases_df: {purchases_df.count()} users_df: {users_df.count()}")


# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.

products_df = products_df.dropna()
purchases_df = purchases_df.dropna()
users_df = users_df.dropna()
print(f"Count after dropna Datasets: "
      f"products_df:{products_df.count()} purchases_df: {purchases_df.count()} users_df: {users_df.count()}")

# 3. Визначте загальну суму покупок за кожною категорією продуктів.

products_df = products_df.dropDuplicates(['product_id'])
purchase_sum_all = purchases_df.select('product_id', 'quantity') \
                      .join(products_df, purchases_df.product_id == products_df.product_id, 'left') \
                      .drop(purchases_df.product_id) \
                      .filter(products_df.category.isNotNull()) \
                      .withColumn("totalCount", round(col('quantity') * col('price'), 2))  \
                      .groupBy('category') \
                      .sum('totalCount')  \
                      .withColumnRenamed('sum(totalCount)', 'total_sum') \
                      .withColumn('total_sum', round(col('total_sum'), 2))  # Round after summing
print('Загальна сума покупок за кожною категорією продуктів:')
purchase_sum_all.show()

# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.

purchase_sum_age = purchases_df.select('product_id', 'user_id', 'quantity') \
                      .join(products_df, 'product_id', 'left') \
                      .join(users_df, 'user_id', 'left') \
                      .where((col('age') >= 18) & (col('age') <= 25)) \
                      .dropna().withColumn("totalCount", col('quantity') * col('price'))\
                      .groupBy('category') \
                      .sum('totalCount')  \
                      .withColumnRenamed('sum(totalCount)', 'total_sum') \
                      .withColumn('total_sum', round(col('total_sum'), ROUND_DIGITS))
print('Загальна сума покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно:')
purchase_sum_age.show()

# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.

purchase_sum_age_percent = purchases_df.select('product_id', 'user_id', 'quantity') \
                            .join(products_df, 'product_id', 'left') \
                            .join(users_df, 'user_id', 'left') \
                            .where((col('age') >= 18) & (col('age') <= 25)) \
                            .dropna().withColumn("totalCount", col('quantity') * col('price'))\
                            .groupBy('category') \
                            .sum('totalCount')  \
                            .withColumnRenamed('sum(totalCount)', 'total_sum') \
                            .withColumn('total_sum', round(col('total_sum'), ROUND_DIGITS)) \

total_count = purchase_sum_age_percent.agg(sum('total_sum').alias('total_count')).collect()[0]['total_count']
purchase_sum_age_percent = purchase_sum_age_percent.withColumn('percentage',
                                                   round(col('total_sum') / total_count * 100, ROUND_DIGITS))
print('Частка покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно:')
purchase_sum_age_percent.show()

# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.

print('Топ 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років:')
top_3_categories = purchase_sum_age_percent.orderBy(desc('percentage')).limit(3)
top_3_categories.show()
