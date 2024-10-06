from delta import *


def main(arg1_conn,arg2_jar,arg3_sparksess):
    print("################################# Create spark session #########################")
    spark=get_spark('Delta',arg3_sparksess,arg2_jar)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("drop database if exists prosparkc cascade");
    spark.sql("create database  prosparkc");
    spark.sql("drop database if exists prosparkd cascade");
    spark.sql("create database prosparkd ");
    spark.sql("create database if not exists prosparkdim ");

    print("#################################Employees Data (Slowly Changing Dimension 2) #########################")
    print("################################# Select only the employees info updated or inserted today in the source DB#########################")
    empquery="(select * from employees where upddt > current_date - interval 1 day)tblquery"
    df_emp=getRDBMS(arg1_conn,spark,'empoffice',empquery)
    print("Employees data")
    df_emp.show(2)
    print("Data Munging")
    df_mung=munge(df_emp,True,True,True,False,False)
    print("After Munging")
    df_mung.show(5)
    print("Checking Table exist or not")
    existflag=checkhive(spark,'prosparkdim','emp')
    print(existflag)
    print("Writing Hive table with CDC SCD2")
    writedatascd2(existflag,spark,"prosparkdim.emp",df_mung)

    print("################################# Select entire Office RDBMS Data #########################")
    df_off=getRDBMS(arg1_conn,spark,'empoffice','offices')
    print("Offices data")
    df_off.show(2)
    print("Munging office data")
    off_mung=munge(df_off,True,True,True,False,False)
    print("After Munging")
    off_mung.show(5)
    print("Writing into hive table")
    whive(off_mung,'prosparkdim.offices',False,'','overwrite')
    off_mung.createOrReplaceTempView("offices")

    print("################################# Select joined Customer & Payments Data (Apply Partition and Pushdown Optimization) #########################")

    custquery="""(select c.customerNumber customernumber,upper(c.customerName) custname,c.country,c.salesRepEmployeeNumber,c.creditLimit,p.paymentDate,p.amount,current_date datadt from customers c inner join payments p on c.customerNumber=p.customerNumber and year(p.paymentdate)=2022 and month(p.paymentdate)>=07 ) query"""

    df_pay=getRDBMSpart(spark,arg1_conn,'custpayments',custquery,1,100,'customernumber',4)
    print("Payments data")
    df_pay.show(2)
    print("Optimizing payment data")
    df_opt=opt(spark,df_pay,4,True,True,10)
    print("Overwrite Hive table prosparkc.custpay")
    whive(df_opt,'prosparkc.custpay',True,'datadt','overwrite')
    df_opt.createOrReplaceTempView("custpayments")

    print("################################# Orders & orderdetails Data #########################")

    orderdetailquery="""(select o.customerNumber,o.orderNumber,o.orderdate,o.shippedDate,o.status,o.comments,od.quantityOrdered,od.priceEach,od.orderLineNumber,od.productCode,current_date as datadt from orders o inner join orderdetails od on o.orderNumber=od.orderNumber and year(o.orderDate)=2022 and month(o.orderDate)>=07) orders"""

    df_ord=getRDBMSpart(spark,arg1_conn,'ordersproducts',orderdetailquery,1,100,"customernumber",4)
    print("Order detail table")
    df_ord.show(5)
    print("Optimizing order detail table")
    df_ord_opt=opt(spark,df_ord,4,True,True,10)
    df_ord_opt.show(2)
    print("Writing in to Hive table")
    whive(df_ord_opt,'prosparkc.orddetail',True,'datadt','overwrite')

    print("################################# Products Data (Apply Delta lake merge to update if products exists else insert) #########################")
    print("################################# Product Profit value, Profit percent, promotion indicator and demand indicator KPI Metrics #########################")
    prodquery="(select * from products where upddt>current_date-interval 1 day) tblquery"
    df_prod_new=getRDBMS(arg1_conn,spark,'ordersproducts',prodquery)
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType,ShortType,StructField,DecimalType
    returnstruct=StructType([StructField("profit",DecimalType(10,2),True),StructField("profitpercent",DecimalType(10,2),True),StructField("promoind",ShortType(),True),StructField("demandind",ShortType(),True)])
    udfpromo=udf(udfprofpromo,returnstruct) 
    udf_df=df_prod_new.select("productCode","productName","productLine","productScale","productVendor","productDescription","quantityInStock","buyPrice","MSRP","upddt",udfpromo(col("buyPrice"),col("MSRP"),col("quantityInStock")).alias("promoprof"))
    udf_df.printSchema()
    df_udf=udf_df.select("productCode","productName","productLine","productScale","productVendor","productDescription","quantityInStock","buyPrice","MSRP","upddt","promoprof.profit","promoprof.profitpercent","promoprof.promoind","promoprof.demandind")

    print("UDF applied Products with Profit ratio, profit percent, promo indicator and demand indicator")
    df_udf.show(3)
    print("Checking Tablle exist")
    tableflag=checkhive(spark,'prosparkdim','hiveprod')
    print(tableflag)
    print("(Apply Delta lake merge to update if products exists else insert into retail_dim.hive_products)")
    print("Writing into Hive with scd1")
    df_merge=writedatascd1(tableflag,spark,'prosparkdim.hiveprod',df_udf)

    print("################################# Data Wrangling ie joining of merged latest products with the orders #########################")

    df_orders=wrangledata(df_merge,df_ord_opt)
    df_orders.show(5,False)
    print("Overwrite Hive Table prosparkc.ord_prod")
    whive(df_orders,'prosparkc.ord_prod',True,"datadt","overwrite")

    print("################################# Custnavigation nested json Data parsing with custom schema #########################")

    strtype=StructType([StructField("id",StringType(),True),StructField("comments",StringType(),True),StructField("pagevisit",ArrayType(StringType()),True)])
    print("Read Cust navigation json Data")
    df_navi=rdata(spark,'json',"file:///home/hduser/Spark_pro/cust_navigation.json",strtype)
    df_navi.createOrReplaceTempView("cust_navigation")

    print("################# Customer Navigation Curation load to hive with positional explode #########################")
    print("Create hive external table using hql, insert into the table and write the curated data into json")
    spark.sql("""create external table if not exists prosparkc.cust_navi (customernumber string,navigation_index int,navigation_pg string) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/user/hduser/custnavi/' """)
    spark.sql("insert into table prosparkc.cust_navi select id,idx,navi from cust_navigation lateral view posexplode(pagevisit) exploded as idx,navi")
    spark.sql("select * from cust_navigation").show(5,False)
    report1=spark.sql("""select c1.navigation_pg,count(distinct c1.customernumber) custcnt,'lastpage' pagevisit from prosparkc.cust_navi c1 inner join (select a.customernumber,max(a.navigation_index)as max_navi from prosparkc.cust_navi a group by customernumber) as c2 on (c1.customernumber=c2.customernumber and c1.navigation_index=c2.max_navi) group by c1.navigation_pg union all select navigation_pg,count(distinct customernumber) custcnt,'firstpage' pagvisit from prosparkc.cust_navi where navigation_index=0 group by navigation_pg""")
    curdt=spark.sql("select date_format(current_date(),'yyyyMMdd')").first()[0]
    wdata('json',report1.coalesce(1),"/user/hduser/prosparkcurated/first_last"+curdt,"overwrite")
    print("Customer Navigation dataframe")
    report1.show(3)
    print("################# Dim Order table External table load using load command #########################")
    spark.sql("""create external table if not exists prosparkd.order_rate(rid varchar(200),orddesc varchar(200),comp_cust varchar(10),siverity int,intent varchar(100)) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/user/hduser/dimorders/'""")
    spark.sql("""load data local inpath 'file:///home/hduser/Spark_pro/orders_rate.csv' overwrite into table prosparkd.order_rate""")

    print("################# Employee Rewards Discovery load #########################")
    
    spark.sql("""select o.*,e.*,c.* from offices o inner join prosparkdim.emp e inner join custpayments c on o.officecode=e.officecode and e.employeeNumber=c.salesRepEmployeeNumber""").createOrReplaceTempView("ceov")

    df_reward=spark.sql("""select * from (select email,sumamt,rank() over (partition by state order by sumamt desc) as rnk,current_date() as datadt from (select email,state,sum(amount) sumamt from ceov where datadt>=current_date() group by email,state)temp1)temp2 where rnk=1""")
    df_reward.show(4,False)
    print("Overwrite Hive table prosparkd.employeerewards to send the rewards to high performing employees")
    whive(df_reward,'prosparkd.empreward',True,'datadt','overwrite')
    
    print("################# Customer Frustration Discovery joining cust_navigation and retail_dim.dim_order_rate tables #########################")
    frustruated_df=spark.sql("""select customernumber,total_siv,case when total_siv between -10 and -3 then 'Highfrustruated' when total_siv between -2 and -1 then 'lowfrustruated' when total_siv=0 then 'neutral' when total_siv between 1 and 2 then 'happy' when total_siv between 3 and 10 then 'overhappy'end as cust_flevel from (select customernumber,sum(siverity) as total_siv from (select o.id as customernumber,o.comments,r.orddesc,r.siverity from cust_navigation o left outer join prosparkd.order_rate r where o.comments like concat('%',r.orddesc,'%')) temp1 group by customernumber) temp2""")

    print("################# Convert DF to frustrationview tempview #########################")
    frustruated_df.createOrReplaceTempView("frustview")
    
    print("Overwrite Hive table retail_discovery.cust_frustration_level to store the customer frustration levels")
    spark.sql("""create external table if not exists prosparkd.cust_flevel(customernumber string,total_siv int,frust_level string) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/user/hduser/custfrust/'""")
    print("Writing Customer frustration data into Hive")
    spark.sql("""insert overwrite table prosparkd.cust_flevel select * from frustview""")

    print("Writing Customer frustration data into RDBMS")
    writeRDBMS(frustruated_df,arg1_conn,'sparkegg','cust_frust','overwrite')
    print("Writing Customer frustration data into S3 Bucket Location")
    #sc = spark.sparkContext
    #sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA3YF5CC3LQDFLDUMU")
    #sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    #write_data('csv',frustrateddf,"s3a://com.iz.test1/IZ_datalake/",'append',',',True)
    print("################################# Application completed successfully #########################")

if __name__=='__main__':
    print("################################# Starting the main application #########################")
    import sys
    if len(sys.argv)==4:
        print("################################# Initializing the Dependent modules #########################")
        from pyspark.sql.types import *
        from project.commonutils import *
        from project.ETLfun import *
        print("Calling the main method with {0}, {1}, {2},{3}".format(sys.argv[0],sys.argv[1],sys.argv[2],sys.argv[3]))
        main(sys.argv[1],sys.argv[2],sys.argv[3])
    else:
        print('Not enough args')
