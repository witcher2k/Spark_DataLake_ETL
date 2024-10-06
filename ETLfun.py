from pyspark.sql.functions import *
from pyspark.sql.window import Window
from project.commonutils import *
from delta.tables import *
from delta import *



def writedatascd2(tableexistflag,sparksess,hivetbl,df_employees_new_updated):
    if tableexistflag:
        print("hive_employees table exists")
        df_hive_existing = readhive(sparksess,hivetbl).groupBy("employeeNumber").agg(max("ver").alias("max_ver"))
        if df_hive_existing.count() > 0:
            joined_df = df_employees_new_updated.alias("new").join(df_hive_existing.alias("exist"), on="employeeNumber",
                                                                   how="left")#lookup and enrichment (Data wrangling)
            joined_df_exist = joined_df \
                .select("new.employeeNumber", "new.lastName", "new.firstName", "new.extension",
                        "new.email", "new.officeCode", "new.reportsTo", "new.jobTitle", "new.upddt", "new.leaveflag",
                        "exist.max_ver") \
                .withColumn("ver", expr(
                "row_number() over(partition by employeeNumber order by upddt) + coalesce(max_ver,0)")).drop("max_ver")
            #                .where("new.employeeNumber is not null")\
            joined_df_exist.show(10)
            #writeToHiveTable(joined_df_exist,"Append","hive",hive_table)
            joined_df_exist.createOrReplaceTempView("empnewexist")
            print("employee exist and the old+new data is..")
            sparksess.sql("select * from empnewexist").show(5, False)
            sparksess.sql("insert into prosparkdim.emp select * from empnewexist")
    else:
        df_employees_new_updated=df_employees_new_updated.withColumn("ver", lit(1))#Data Enrichment
        df_employees_new_updated.createOrReplaceTempView("empnew")
        print("employee table not exist and the new data is..")
        sparksess.sql("select * from empnew").show(5,False)
        sparksess.sql("""CREATE external TABLE prosparkdim.emp(
        `employeenumber` int,`lastname` string,`firstname` string,`extension` string,`email` string, 
        `officecode` string, `reportsto` int,`jobtitle` string, `upddt` date, `leaveflag` string,`ver` int)
        row format delimited fields terminated by ','
        location 'hdfs://localhost:54310/user/hduser/empdata/'""")
        sparksess.sql("insert into prosparkdim.emp select * from empnew")
        #writeToHiveTable(df_employees_new_updated, "Overwrite", "hive", hive_table)
        print("table doesn't exists, hence created and loaded the data")


orderprodquery="""(select o.customerNumber,o.orderNumber,o.orderDate,o.requiredDate,o.shippedDate,o.status,o.comments,od.productCode,od.quantityOrdered,od.priceEach,od.orderLineNumber,p.productName,p.productLine,p.productScale,p.productVendor,p.productDescription,p.quantityInStock,p.buyPrice,p.MSRP from orders o inner join orderdetails od on o.orderNumber=od.orderNumber inner join products p inner join on od.productCode=p.productCode and year(o.orderDate)=2022 and month(o.orderDate)=10) ordquery"""

def wrangledata(df,orderprodquery):
    df=df.alias("p").join(orderprodquery.alias("o"),on="productCode",how="inner")
    if __name__=="__main__":
        df=df.withColumn(row_number().over(Window.orderBy(desc("orderdate"))).alias("rno")).select("p.MSRP", "p.buyPrice", "o.comments", "o.customerNumber", "o.orderdate",
         "o.orderlinenumber","o.orderNumber", "o.priceEach", "p.productCode", "p.productDescription", "p.productLine","p.productName", "p.productScale", "p.productVendor", "p.quantityInStock", "o.quantityordered","o.shippeddate", "o.status")
    return df


def udfprofpromo(sp,cp,qty):
    profit=sp-cp
    profitpercent=(profit / cp) * 100
    if profit > 0 :
        profind=1
    else:
        profind=0
    if qty>0 and qty<1500 and profitpercent>50.0:
        demandind=1
    else:
        demandind=0
    if qty>1500 and profitpercent>20.0:
        promoind=1
    else:
        promoind=0
    return profit,profitpercent,demandind,promoind



from delta.tables import DeltaTable

def writedatascd1(tableexistflag, sparksess, hivetbl, df):
    if tableexistflag:
        hive_df = readhive(sparksess, hivetbl)
        hive_df.write.format("delta").mode("overwrite").save("/user/hduser/hive_exist")
        exist_df = DeltaTable.forPath(sparksess, "/user/hduser/hive_exist")

        # Correctly specify the condition for merging
        exist_df.alias("exist").merge(
            source=df.alias("new"),
            condition=expr("new.productCode = exist.productCode")
        ).whenMatchedUpdate(set={
            "productName": col("new.productName"),
            "productLine": col("new.productLine"),
            "productScale": col("new.productScale"),
            "productVendor": col("new.productVendor"),
            "productDescription": col("new.productDescription"),
            "quantityInStock": col("new.quantityInStock"),
            "buyPrice": col("new.buyPrice"),
            "MSRP": col("new.MSRP"),
            "upddt": col("new.upddt"),
            "profit": col("new.profit"),
            "profitpercent": col("new.profitpercent"),
            "promoind": col("new.promoind"),
            "demandind": col("new.demandind")
        }).whenNotMatchedInsert(values={
            "productCode": col("new.productCode"),
            "productName": col("new.productName"),
            "productLine": col("new.productLine"),
            "productScale": col("new.productScale"),
            "productVendor": col("new.productVendor"),
            "productDescription": col("new.productDescription"),
            "quantityInStock": col("new.quantityInStock"),
            "buyPrice": col("new.buyPrice"),
            "MSRP": col("new.MSRP"),
            "upddt": col("new.upddt"),
            "profit": col("new.profit"),
            "profitpercent": col("new.profitpercent"),
            "promoind": col("new.promoind"),
            "demandind": col("new.demandind")
        }).execute()

        complete_df = exist_df.toDF()
        complete_df.createOrReplaceTempView("completetbl")  # Use createOrReplaceTempView instead of registerTempTable
        sparksess.sql("DROP TABLE IF EXISTS prosparkdim.hiveprod")
        sparksess.sql("""
            CREATE TABLE IF NOT EXISTS prosparkdim.hiveprod (
                productCode STRING,
                productName STRING,
                productLine STRING,
                productScale STRING,
                productVendor STRING,
                productDescription STRING,
                quantityInStock INT,
                buyPrice DECIMAL(10,2),
                MSRP DECIMAL(10,2),
                upddt DATE,
                profit DECIMAL(10,2),
                profitpercent DECIMAL(10,2),
                promoind SMALLINT,
                demandind SMALLINT
            )
        """)
        sparksess.sql("INSERT OVERWRITE TABLE prosparkdim.hiveprod SELECT * FROM completetbl")
        return complete_df
    else:
        print("Table does not exist")
        df.createOrReplaceTempView("tbl")  # Use createOrReplaceTempView instead of registerTempTable
        sparksess.sql("SELECT * FROM tbl").show(3)
        sparksess.sql("DROP TABLE IF EXISTS prosparkdim.hiveprod")
        sparksess.sql("""
            CREATE TABLE IF NOT EXISTS prosparkdim.hiveprod (
                productCode STRING,
                productName STRING,
                productLine STRING,
                productScale STRING,
                productVendor STRING,
                productDescription STRING,
                quantityInStock INT,
                buyPrice DECIMAL(10,2),
                MSRP DECIMAL(10,2),
                upddt DATE,
                profit DECIMAL(10,2),
                profitpercent DECIMAL(10,2),
                promoind SMALLINT,
                demandind SMALLINT
            )
        """)
        sparksess.sql("INSERT INTO TABLE prosparkdim.hiveprod SELECT * FROM tbl")
        return df





    
