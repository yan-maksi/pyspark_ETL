from functools import reduce
from sre_parse import Tokenizer

import pyspark.sql.functions as F
import stopwordsiso
from pyspark.sql import SparkSession


class Start_spark_session:
    """ spark session from dict configuraton """
    try:
        def createBuilder(master: str, appname: str, config: dict) -> SparkSession.Builder:
            """ create a spark session """
            builder = SparkSession \
                .builder \
                .appName(appname) \
                .master(master)
            return configDeltalake(builder, config)

        def configDeltalake(builder: SparkSession.Builder, config: dict) -> SparkSession.Builder:
            """ add delta lake to your session """
            if isinstance(builder, SparkSession.Builder) and config.get('deltalake') == True:
                from delta import configure_spark_with_delta_pip
                builder \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                return configure_spark_with_delta_pip(builder)
            return builder

        def createSession(builder: SparkSession.Builder) -> SparkSession:
            if isinstance(builder, SparkSession.Builder):
                return builder.getOrCreate()

        def setLogging(spark: SparkSession, log_level: str) -> None:
            """ set log level - ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
                this function will overide the configuration also set in log4j.properties
                    for example, log4j.rootCategory=ERROR, console
            """
            if isinstance(spark, SparkSession):
                spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None


class Filter_words:
    def __init__(self, sdf):
        self.sdf = sdf



sdf = Filter_words("sdf0")




class Upload_files:
