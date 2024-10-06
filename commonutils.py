import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from configparser import ConfigParser
from delta import configure_spark_with_delta_pip
from delta.tables import *


def get_spark(typeflag, propfile, jdbc_lib):
    # Ensure JDBC JAR exists
    if not os.path.exists(jdbc_lib):
        print(f"Error: JDBC library {jdbc_lib} not found!")
        sys.exit(1)

    # Load configurations from the property file
    spark_config = SparkConf()
    config = ConfigParser()
    config.read(propfile)

    for i, j in config.items("CONFIGS"):
        spark_config.set(i, j)

    try:
        if typeflag == 'Delta':
            # Adding Delta-specific configurations
            build = (SparkSession.builder
                     .appName("spark")
                     .config("spark.driver.extraClassPath", jdbc_lib)
                     .config("spark.executor.extraClassPath", jdbc_lib)
                     .config("spark.jars", jdbc_lib)
                     .config(conf=spark_config)
                     # Delta Lake configs
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .enableHiveSupport())

            # Configure with Delta
            spark = configure_spark_with_delta_pip(build).getOrCreate()

            print("Spark session with Delta Lake created successfully.")
            return spark

        else:
            # Standard non-Delta Spark session
            build = (SparkSession.builder
                     .appName("spark")
                     .config("spark.driver.extraClassPath", jdbc_lib)
                     .config("spark.executor.extraClassPath", jdbc_lib)
                     .config("spark.jars", jdbc_lib)
                     .config(conf=spark_config)
                     .enableHiveSupport())

            spark = build.getOrCreate()

            print("Non-Delta Spark session created successfully.")
            return spark
    except Exception as Sparkerror:
        print(f"Spark error: {Sparkerror}")
        sys.exit(1)



#readdatas
def rdata(sparksess,typeflag,tar,sch,delim=',',head=False,isch=False):
    if typeflag=='csv' and sch is not None:
        df1=sparksess.read.csv(tar,schema=sch,sep=delim,header=head)
        return df1
    elif typeflag=='csv':
        df1=sparksess.read.csv(tar,inferSchema=isch,sep=delim,header=head)
        return df1
    elif typeflag=='json':
        df1=sparksess.read.option("multiline","true").schema(sch).json(tar)
        return df1
#write data 6
def wdata(typeflag,df,tar,head=False,delim=',',mod='overwrite'):
    if typeflag=='csv':
        df.write.csv(tar,mode=mod,header=head,sep=delim)
    elif typeflag=='json':
        df.write.mode(mod).option("multiline","true").json(tar)



from pyspark.sql.functions import lower, col


def checkhive(sparksess, db, tblname):
    # Get all tables in the database
    tables_df = sparksess.sql(f"show tables in {db}")
    tables_df.show()  # Show all tables in the database for debugging

    # Convert both table name and filter condition to lowercase for case-insensitive match
    if tables_df.filter(lower(col("tableName")) == tblname.lower()).count() > 0:
        return True
    else:
        return False


#reaadtbl
def readhive(sparksess,tblname):
    df2=sparksess.read.table(tblname)
    return df2
#writehivetbl
def whive(df,tblname,partflag,partcol,mod):
    if partflag==False:
        df.write.mode(mod).saveAsTable(tblname)
    else:
        df.write.mode(mod).partitionBy(partcol).saveAsTable(tblname)
#getrdbmspart
from configparser import ConfigParser
from pyspark.sql import SparkSession


def getRDBMSpart(sparksess,propfile,db,tbl,lowerbound,upperbound,partcol,numpart):
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DEV", 'driver')
    host=config.get("DEV", 'host')
    port=config.get("DEV", 'port')
    user=config.get("DEV", 'user')
    passwd=config.get("DEV", 'pass')
    url=host+":"+port+"/"+db

    db_df=sparksess.read.format("jdbc").option("url",url)\
    .option("dbtable",tbl)\
    .option("user",user).option("password",passwd)\
    .option("driver",driver) \
    .option("lowerBound", lowerbound)\
    .option("upperBound", upperbound)\
    .option("numPartitions", numpart)\
    .option("partitionColumn", partcol)\
    .load()
    return db_df



def getRDBMS(propfile,sparksess,db,tbl):#configuration driven approach
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DEV", 'driver')
    host=config.get("DEV", 'host')
    port=config.get("DEV", 'port')
    user=config.get("DEV", 'user')
    passwd=config.get("DEV", 'pass')
    url = f"{host}:{port}/{db}"
    #jdbc:mysql://127.0.0.1:3307/empoffice
    db_df=sparksess.read.format("jdbc").option("url",url)\
    .option("dbtable",tbl)\
    .option("user",user).option("password",passwd)\
    .option("driver",driver).load()
    return db_df

#writerdbms
def writeRDBMS(df,propfile,db,tbl,mod="overwrite"):
    config=ConfigParser()
    config.read(propfile)
    driver=config.get("DEV","driver")
    port=config.get("DEV","port")
    host=config.get("DEV","host")
    user=config.get("DEV","user")
    passw=config.get("DEV","pass")
    url=host+":"+port+"/"+db 
    url1=url+"?"+"user="+user+"&password="+passw
    df.write.jdbc(url=url1,mode=mod,table=tbl,properties={"driver":driver})
    
#opt 
def opt(sparksess,df,numpart,partflag,cacheflag,numshuff=200):
    if partflag:
        df=df.repartition(numpart)
    else:
        df=df.coalesce(numpart)
    if cacheflag:
        df=df.cache()
    elif numshuff!=200:
        sparksess.conf.set("spark.sql.shuffle.partitions",numshuff)
    return df
#munge
def munge(df,dedup,naall,naany,nasubany,nasuball,sub=None):
    if dedup:
        df=df.dropDuplicates()
    elif naall:
        df=df.na.drop('all')
    elif naany:
        df=df.na.drop('any')
    elif nasubany:
        df=df.na.drop('any',subset=sub)
    elif nasuball:
        df=df.na.drop('all',subset=sub)
    return df
    