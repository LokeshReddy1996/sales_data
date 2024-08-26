import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.types import StringType,FloatType,IntegerType

def read_data(file_path,password):
    spark=SparkSession.builder.appName("read csv file").getOrCreate()
    df=spark.read.option("header","true").option("inferSchema","true").csv(file_path)
    return df

def transform_data(df,region):
    df=df.withColumn("region",col("region").cast(StringType())).withColumn("total_sales",col("QuantityOrdered").cast(IntegerType())*col("ItemPrice").cast(FloatType())).withColumn("net_sales",col("total_sales")-col("PromotionDiscount").cast((FloatType())))
    df=df.withColumn("region",when(col("region").isNull(),"region").otherwise(col("region")))
    df=df.filter(col("net_sale")>0)
    return df.dropDuplicates(["OrderId"])


def create_table(conn):
    cursor=conn.cursor()
    cursor.execute('''
        create table if not exists sales_data(
            OrderId text primary key,
            OrderItemId text,
            QuantityOrdered integer,
            ItemPrice real,
            PromotionDiscount real,
            total_sales real,
            region text,
            net_sale real
        )
    ''')

def load_data_to_db(df,db_path):
    conn=sqlite3.connect(db_path)
    create_table(conn)
    df.write.format("jdbc").option("url",f"jdbc:sqlite:{db_path}").option("dbtable","sales_data").option("driver","org.sqlite.JDBC").mode("append").save()
    conn.close()

def validate_data(db_path):
    conn=sqlite3.connect(db_path)
    cursor=conn.cursor()

    # Total number of records
    cursor.execute("select count(*) from sales_data")
    total_record=cursor.fetchone()[0]

    # Total sales amount by region
    cursor.execute("select region,sum(total_sales) from sales_data group by region")
    total_sales_by_region=cursor.fetchall()

    #Average sales amount per transaction
    cursor.execute("select avg(total_sales) from sales_data")
    avg_sales=cursor.fetchone()[0]

    # check for duplicate OrderId
    cursor.execute("select OrderId,count(*) from sales_data group by OrderId having count(*)>1")
    duplicates=cursor.fetchall()

    conn.close()

    return total_record,total_sales_by_region,avg_sales,duplicates


def main():
    file_region_a = "C:/Users/User/PycharmProjects/Sales_order/order_region_a.csv"
    file_region_b = "C:/Users/User/PycharmProjects/Sales_order/order_region_b.csv"

    #read data
    df_a=read_data(file_region_a,"order_region_a")
    df_b=read_data(file_region_b,"order_region_b")

    #Transform_data
    df_a_transformed=transform_data(df_a,"A")
    df_b_transformed=transform_data(df_b,"B")

    #combine data
    combined_df=df_a_transformed.union(df_b_transformed)

    # Load data to SQLite
    db_path="sales_data.db"
    load_data_to_db(combined_df,db_path)

    # validate data
    total_record,total_sales_by_region,avg_sales,duplicates=validate_data(db_path)

    print("Total records:",total_record)
    print("Total sales by region",total_sales_by_region)
    print("Average sales per transaction",avg_sales)
    print("Duplicate order id",duplicates)


if __name__=="__main__":
    main()

