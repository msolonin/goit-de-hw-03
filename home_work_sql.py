from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, desc

ROUND_DIGITS = 2
spark = SparkSession.builder.appName("HomeWork1").getOrCreate()

# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.

products_df = spark.read.csv('products.csv', header=True)
purchases_df = spark.read.csv('purchases.csv', header=True)
users_df = spark.read.csv('users.csv', header=True)

# Створюємо тимчасове представлення для виконання SQL-запитів, якщо ми хочимо те сам зробити в SQL запитах:
products_df.createTempView("products")
purchases_df.createTempView("purchases")
users_df.createTempView("users")


# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.

products_df_sql = spark.sql("""SELECT * FROM products WHERE product_id IS NOT NULL 
                                                      AND product_name IS NOT NULL 
                                                      AND category IS NOT NULL 
                                                      AND price IS NOT NULL;""")

purchases_df_sql = spark.sql("""SELECT * FROM purchases WHERE purchase_id IS NOT NULL 
                                                      AND user_id IS NOT NULL 
                                                      AND product_id IS NOT NULL 
                                                      AND date IS NOT NULL 
                                                      AND quantity IS NOT NULL;""")
users_df_sql = spark.sql("""SELECT * FROM users WHERE user_id IS NOT NULL 
                                                      AND name IS NOT NULL 
                                                      AND age IS NOT NULL 
                                                      AND email IS NOT NULL;""")
print(f"Count after dropna Datasets: "
      f"products_df_sql:{products_df_sql.count()} purchases_df_sql: {purchases_df_sql.count()} users_df_sql: {users_df_sql.count()}")

products_df_sql.createTempView("products_df")
purchases_df_sql.createTempView("purchases_df")
users_df_sql.createTempView("users_df")

# 3. Визначте загальну суму покупок за кожною категорією продуктів.

purchase_sum_all_sql = spark.sql(f"""SELECT products_df.category,
                                    ROUND(SUM(purchases_df.quantity * products_df.price), {ROUND_DIGITS}) AS total_sum
                                    FROM purchases_df
                                    LEFT JOIN  products_df
                                    ON purchases_df.product_id = products_df.product_id
                                    WHERE products_df.category IS NOT NULL
                                    GROUP BY products_df.category;""")

print('Загальна сума покупок за кожною категорією продуктів SQL:')
purchase_sum_all_sql.show()

# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.

purchase_sum_age = spark.sql(f"""SELECT products_df.category,
                                    ROUND(SUM(purchases_df.quantity * products_df.price), {ROUND_DIGITS}) AS total_sum
                                    FROM purchases_df
                                    LEFT JOIN  products_df
                                    ON purchases_df.product_id = products_df.product_id
                                    LEFT JOIN  users_df
                                    ON purchases_df.user_id = users_df.user_id
                                    WHERE products_df.category IS NOT NULL
                                    AND users_df.age BETWEEN 18 AND 25
                                    GROUP BY products_df.category;""")

print('Загальна сума покупок за кожною категорією продуктів SQL:')
purchase_sum_age.show()


# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.

total_count = purchase_sum_age.agg(sum('total_sum').alias('total_count')).collect()[0]['total_count']
purchase_sum_age_percent = spark.sql(f"""SELECT products_df.category,
                        ROUND(SUM(purchases_df.quantity * products_df.price), {ROUND_DIGITS}) AS total_sum,
                        ROUND((SUM(purchases_df.quantity * products_df.price) / {total_count}) * 100, 2) AS percentage
                        FROM purchases_df
                        LEFT JOIN products_df
                        ON purchases_df.product_id = products_df.product_id
                        LEFT JOIN users_df
                        ON purchases_df.user_id = users_df.user_id
                        WHERE products_df.category IS NOT NULL
                        AND users_df.age BETWEEN 18 AND 25
                        GROUP BY products_df.category;""")

print('Загальна сума покупок за кожною категорією продуктів і їх відсоток:')
purchase_sum_age_percent.show()

# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
top_3_categories = spark.sql(f"""SELECT products_df.category,
                        ROUND(SUM(purchases_df.quantity * products_df.price), {ROUND_DIGITS}) AS total_sum,
                        ROUND((SUM(purchases_df.quantity * products_df.price) / {total_count}) * 100, 2) AS percentage
                        FROM purchases_df
                        LEFT JOIN products_df
                        ON purchases_df.product_id = products_df.product_id
                        LEFT JOIN users_df
                        ON purchases_df.user_id = users_df.user_id
                        WHERE products_df.category IS NOT NULL
                        AND users_df.age BETWEEN 18 AND 25
                        GROUP BY products_df.category
                        ORDER BY percentage 
                        DESC LIMIT 3;""")
print('Топ 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років:')
top_3_categories.show()