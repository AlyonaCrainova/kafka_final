package org.example.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Spark-аналитика по событиям из HDFS:
 * <ul>
 *   <li>статистика по товарам: просмотры, корзина, покупки и популярность (просмотры + корзина + покупки);</li>
 *   <li>рекомендации: для каждого пользователя — до 5 самых популярных товаров, которые он ещё не смотрел.</li>
 * </ul>
 * Запуск: spark-submit --class org.example.analytics.RecommendationsSparkJob app.jar hdfs://hadoop-namenode:9000
 */
public class RecommendationsSparkJob {

    private static final String PRODUCT_STATS_SQL = """
            SELECT product_id,
                   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
                   SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
                   SUM(CASE WHEN event_type = 'buy' THEN 1 ELSE 0 END) AS purchases,
                   SUM(CASE WHEN event_type IN ('view', 'cart', 'buy') THEN 1 ELSE 0 END) AS popularity
            FROM events
            WHERE product_id IS NOT NULL
            GROUP BY product_id
            """;

    private static final String RECOMMENDATIONS_SQL = """
            WITH users AS (
                SELECT DISTINCT user_id FROM events WHERE user_id IS NOT NULL
            ),
            top_products AS (
                SELECT product_id, popularity FROM product_stats
                ORDER BY popularity DESC, product_id
                LIMIT 5
            ),
            candidates AS (
                SELECT u.user_id, t.product_id, t.popularity
                FROM users u
                CROSS JOIN top_products t
                WHERE NOT EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.user_id = u.user_id AND e.product_id = t.product_id
                )
            )
            SELECT user_id,
                   transform(sort_array(collect_list(named_struct('neg_popularity', -popularity, 'product_id', product_id)), true),
                             x -> x.product_id) AS recommended_products,
                   current_timestamp() AS generated_at
            FROM candidates
            GROUP BY user_id
            """;

    public static void main(String[] args) {
        String base = args.length > 0 ? args[0] : "hdfs://hadoop-namenode:9000";

        SparkSession spark = SparkSession.builder().appName("shop-recommendations").getOrCreate();
        try {
            spark.read().json(base + "/data/user-events").createOrReplaceTempView("events");

            Dataset<Row> stats = spark.sql(PRODUCT_STATS_SQL);
            stats.write().mode(SaveMode.Overwrite).json(base + "/data/analytics/product_stats");
            stats.createOrReplaceTempView("product_stats");

            spark.sql(RECOMMENDATIONS_SQL).coalesce(1).write().mode(SaveMode.Overwrite)
                    .json(base + "/data/recommendations");

            System.out.println("Аналитика и рекомендации записаны в HDFS: " + base + "/data/recommendations");
        } finally {
            spark.stop();
        }
    }
}
