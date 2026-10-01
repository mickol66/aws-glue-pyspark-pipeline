import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, upper, current_timestamp

# 1. Hämta argument som skickats med från CloudFormation/Glue-konfigurationen
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'LANDING_ZONE_BUCKET', 'ANALYTICS_ZONE_BUCKET'])

# 2. Initiera Spark och Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Hämta bucket-namnen dynamiskt från argumenten
landing_bucket = args['LANDING_ZONE_BUCKET']
analytics_bucket = args['ANALYTICS_ZONE_BUCKET']

print(f"🚀 Startar ETL-process. Läser från: s3://{landing_bucket}")

try:
    # 📥 3. EXTRACT: Läs in rå JSON-data från Landing Zone (S3) till en Glue DynamicFrame
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"s3://{landing_bucket}/raw_traffic/"]},
        format="json"
    )

    # Konvertera till en vanlig Spark DataFrame för att göra avancerade transformationer
    df = datasource.toDF()
    
    if df.rdd.isEmpty():
        print("⚠️ Landing Zone är tom. Inga filer att bearbeta.")
    else:
        print(f"📊 Hittade data. Antal rader som ska transformeras: {df.count()}")

        # 🔄 4. TRANSFORM: Datatvätt och optimering (Exempel baserat på Chinook-kunder)
        # - Vi säkerställer rätt datatyper (customerid till Integer, total till Double)
        # - Vi gör om länders namn till enbart VERSALER (standardisering)
        # - Vi lägger till en tidsstämpel för när datan tvättades (Ingest Timestamp)
        transformed_df = df \
            .withColumn("customerid", col("customerid").cast("integer")) \
            .withColumn("total", col("total").cast("double")) \
            .withColumn("country", upper(col("country"))) \
            .withColumn("processed_at", current_timestamp())

        # Konvertera tillbaka till en Glue DynamicFrame inför lagringen
        from awsglue.dynamicframe import DynamicFrame
        output_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "output_dynamic_frame")

        # 📤 5. LOAD: Skriv ut den optimerade datan till Analytics Zone i Parquet-format
        # Vi partitionerar datan på 'country' (Best Practice för snabbare och billigare Athena-frågor!)
        print(f"💾 Sparar transformerad data till Parquet i: s3://{analytics_bucket}")
        
        glueContext.write_dynamic_frame.from_options(
            frame=output_dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": f"s3://{analytics_bucket}/processed_traffic/",
                "partitionKeys": ["country"] # Parquet delas upp i mappar per land
            },
            format="parquet",
            format_options={"compression": "snappy"} # Högpresterande komprimering
        )
        
        print("✅ ETL-jobbet slutfördes utan fel!")

except Exception as e:
    print(f"❌ Ett fel uppstod i ETL-pipelinen: {str(e)}")
    raise e

finally:
    # Signallera till AWS Glue att jobbet är avslutat
    job.commit()
