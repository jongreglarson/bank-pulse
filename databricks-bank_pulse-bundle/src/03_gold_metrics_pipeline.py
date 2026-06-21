import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="gold_bank_metrics",
    comment="Gold analytics layer with business metrics, peer groupings, and QoQ changes",
    table_properties={"quality": "gold"},
)
def gold_bank_metrics():
    silver = dlt.read("silver_financials")

    bank_window = Window.partitionBy("cert").orderBy("report_date")

    return (
        silver
        .withColumn("quarter_num", F.quarter(F.col("report_date")))
        .withColumn(
            "net_interest_income",
            F.col("interest_income") - F.col("interest_expense"),
        )
        .withColumn(
            "nim",
            F.when(
                F.col("total_assets") > 0,
                (F.col("interest_income") - F.col("interest_expense"))
                / F.col("total_assets") * 100
                * (4.0 / F.col("quarter_num")),
            ).otherwise(None),
        )
        .withColumn("npl_ratio", F.col("nonperforming_assets"))
        .withColumn(
            "peer_group",
            F.when(F.col("total_assets") < 100_000,     F.lit("Community (<$100M)"))
             .when(F.col("total_assets") < 1_000_000,   F.lit("Mid-Size ($100M-$1B)"))
             .when(F.col("total_assets") < 10_000_000,  F.lit("Regional ($1B-$10B)"))
             .when(F.col("total_assets") < 100_000_000, F.lit("Large ($10B-$100B)"))
             .otherwise(F.lit("Mega (>$100B)")),
        )
        .withColumn(
            "peer_group_sort",
            F.when(F.col("total_assets") < 100_000,     F.lit(1))
             .when(F.col("total_assets") < 1_000_000,   F.lit(2))
             .when(F.col("total_assets") < 10_000_000,  F.lit(3))
             .when(F.col("total_assets") < 100_000_000, F.lit(4))
             .otherwise(F.lit(5)),
        )
        .withColumn("assets_prior_qtr",     F.lag("total_assets", 1).over(bank_window))
        .withColumn("nim_prior_qtr",        F.lag("nim", 1).over(bank_window))
        .withColumn("npl_ratio_prior_qtr",  F.lag("npl_ratio", 1).over(bank_window))
        .withColumn("roa_prior_qtr",        F.lag("roa", 1).over(bank_window))
        .withColumn(
            "assets_qoq_chg",
            F.when(
                F.col("assets_prior_qtr") > 0,
                (F.col("total_assets") - F.col("assets_prior_qtr"))
                / F.col("assets_prior_qtr") * 100,
            ).otherwise(None),
        )
        .withColumn("nim_qoq_chg",       F.col("nim")       - F.col("nim_prior_qtr"))
        .withColumn("npl_ratio_qoq_chg", F.col("npl_ratio") - F.col("npl_ratio_prior_qtr"))
        .withColumn("roa_qoq_chg",       F.col("roa")       - F.col("roa_prior_qtr"))
        .drop(
            "assets_prior_qtr", "nim_prior_qtr", "npl_ratio_prior_qtr", "roa_prior_qtr",
            "quarter_num",
        )
    )
