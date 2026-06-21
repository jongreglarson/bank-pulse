import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType
from pyspark.sql.window import Window


@dlt.table(
    name="silver_financials",
    comment="Cleaned and deduplicated FDIC financials; DQ violations tracked as pipeline metrics",
    table_properties={"quality": "silver"},
)
@dlt.expect("non_null_assets", "total_assets IS NOT NULL")
@dlt.expect("positive_assets", "total_assets > 0")
@dlt.expect("non_null_report_date", "report_date IS NOT NULL")
@dlt.expect("non_null_capital_ratio", "tier1_capital_ratio IS NOT NULL")
def silver_financials():
    bronze = dlt.read("bronze_financials")

    silver = bronze.select(
        F.to_date(F.col("REPDTE"), "yyyyMMdd").alias("report_date"),
        F.col("CERT").cast(IntegerType()).alias("cert"),
        F.col("ASSET").cast(LongType()).alias("total_assets"),
        F.col("DEP").cast(LongType()).alias("total_deposits"),
        F.col("LNLSNET").cast(LongType()).alias("net_loans"),
        F.col("INTINC").cast(DoubleType()).alias("interest_income"),
        F.col("EINTEXP").cast(DoubleType()).alias("interest_expense"),
        F.col("NETINC").cast(DoubleType()).alias("net_income"),
        F.col("NPERFV").cast(DoubleType()).alias("nonperforming_assets"),
        F.col("RBC1RWAJ").cast(DoubleType()).alias("tier1_capital_ratio"),
        F.col("ROA").cast(DoubleType()).alias("roa"),
        F.col("ROE").cast(DoubleType()).alias("roe"),
        F.col("ingested_at"),
        F.col("source"),
        F.col("batch_id"),
    )

    dedup_window = Window.partitionBy("cert", "report_date").orderBy(F.col("ingested_at").desc())
    return (
        silver
        .withColumn("_rank", F.row_number().over(dedup_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
