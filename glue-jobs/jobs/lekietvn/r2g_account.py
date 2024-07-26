import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pandas as ps

from calendar import monthcalendar
from datetime import datetime, timedelta
from datetime import date
import holidays

from joblib import variables as V

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# GLOBALS
mapping = {
    'account.csv': 'scd2',
    'account_type.csv': 'sync',
    'customer.csv': 'scd2',
    'payment_transaction.csv': 'sync',
    'payment_type.csv': 'sync'
}

raw_root = f's3://{V.DATA_LANDING_BUCKET_NAME}/kietl'
global_golden_root = f's3://{V.DATA_LANDING_BUCKET_NAME}/golden_zone' # main zone
local_golden_root = f's3://{V.DATA_LANDING_BUCKET_NAME}/kietl/golden_zone' # for backup
    
def initEtl():
    map_dim_table = {}
    
    for s3_file, etl_type in mapping.items():
        if ".csv" not in s3_file: 
            raise ValueError("s3 file must contain .csv extension")
        
        df = None
        
        if etl_type == 'scd2':
            df = etlScd2DimTables(s3_file)
        elif etl_type == 'sync':
            df = etlSyncDimTables(s3_file)
            
        if df is not None:
            key = s3_file.split('.')[0]
            map_dim_table[key] = df
            
    df_dim_date = etlInitDateDimension()
    df_fact = etlInitFactTable(
        map_dim_table['payment_transaction'], map_dim_table['payment_type'], 
        df_dim_date, map_dim_table['account'], map_dim_table['account_type']
    )

def etlSyncDimTables(table_s3_file):
    s3_landing_source = f'{raw_root}/{table_s3_file}'
    output_name = 'kietl_dim_' + table_s3_file.split('.')[0]
    s3_golden_local_dest = f'{local_golden_root}/{output_name}'
    s3_golden_global_dest = f'{global_golden_root}/{output_name}'
    
    df = spark.read.option('header', 'true').option('delimiter', ',').csv(s3_landing_source)
    
    df.write.mode('overwrite').parquet(s3_golden_local_dest)
    df.write.mode('overwrite').parquet(s3_golden_global_dest)
    
    return df

def etlScd2DimTables(table_s3_file):
    s3_landing_source = f'{raw_root}/{table_s3_file}'
    output_name = 'kietl_dim_' + table_s3_file.split('.')[0]
    s3_golden_global_dest = f'{global_golden_root}/{output_name}'
    s3_golden_local_dest = f'{local_golden_root}/{output_name}'
    
    df = spark.read.option('header', 'true').option('delimiter', ',').csv(s3_landing_source) \
        .withColumn('is_active', lit(True)) \
        .withColumn('record_created_time', current_timestamp()) \
        .withColumn('record_updated_time', to_date(lit("3000-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss"))
        
    
    df.write.mode('overwrite').parquet(s3_golden_local_dest)
    df.write.mode('overwrite').parquet(s3_golden_global_dest)

    return df

def etlInitDateDimension(start_date = '2015-01-01', end_date = '2024-12-31'):
    date_list = date_range(start_date, end_date)
    vn_holidays = holidays.VN()  # this is a dict

    def is_holiday(date):
        return date in vn_holidays

    def get_holiday_name(date):
        return vn_holidays.get(date)

    def get_week_of_month(year, month, day):
        return next(
            (
                week_number
                for week_number, days_of_week in enumerate(monthcalendar(year, month), start=1)
                if day in days_of_week
            ),
            None,
        )
        
    udf_is_holiday = udf(is_holiday, BooleanType())
    udf_get_week_of_month = udf(get_week_of_month)
    udf_get_holiday_name = udf(get_holiday_name, StringType())

    df = spark.createDataFrame([(date,) for date in date_list], ['date']) \
        .withColumn('date', to_date(col('date'), 'yyyy-MM-dd')) \
        .withColumn("is_holiday", udf_is_holiday("date")) \
        .withColumn("quarter", quarter("date")) \
        .withColumn("year", date_format("date", "yyyy")) \
        .withColumn("date_key", date_format("date", "yyyyMMdd")) \
        .withColumn("week_of_month", udf_get_week_of_month(year(col('date')), month(col('date')), dayofmonth(col('date')))) \
        .withColumn('holiday_name', when(col('is_holiday') == True, udf_get_holiday_name(col('date'))).otherwise(lit('work day')))
        
    # export to S3
    output_name = 'kietl_dim_date'
    s3_golden_global_dest = f'{global_golden_root}/{output_name}'
    s3_golden_local_dest = f'{local_golden_root}/{output_name}'
    df.write.mode('overwrite').parquet(s3_golden_local_dest)
    df.write.mode('overwrite').parquet(s3_golden_global_dest)

    return df
        
def date_range(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    delta = timedelta(days=1)
    dates_list = []
    while start_date <= end_date:
        dates_list.append(start_date.strftime('%Y-%m-%d'))
        start_date += delta
    return dates_list


def etlInitFactTable(df_dim_payment_transaction, df_dim_payment_type, df_dim_date, df_dim_account, df_dim_account_type):
    df_payment_transaction_full = df_dim_payment_transaction \
        .join(df_dim_payment_type, df_dim_payment_transaction['payment_code'] == df_dim_payment_type['type_code']) \
        .withColumn('transaction_date', to_date('transaction_time'))

    df_payment_transaction_full.createOrReplaceTempView('dim_payment_transaction')
    df_dim_date.createOrReplaceTempView('dim_date')
    df_dim_account.createOrReplaceTempView('dim_account')
    df_dim_account_type.createOrReplaceTempView('dim_account_type')    
    
    
    df_fact = spark.sql("""
        with cte_transaction_revenue as (
            select transaction_date, a.cust_id,
                count(trans_id) as cust_no_transaction_daily,
                sum(amount) as cust_daily_spending,
                collect_list(distinct type_nm) as cust_daily_payment_type
            from dim_payment_transaction pm join dim_account a on pm.acc_id = a.acc_id
            group by transaction_date, a.cust_id
        ),
        cte_cust_accum_revenue as (
            select cust_id,
                sum(cust_daily_spending) over (partition by cust_id order by transaction_date) as cust_accum_spending
            from cte_transaction_revenue
        ),
        cte_account_payment_summary as (
            select pm.transaction_date, cust_id, a.acc_id, at.type_nm as account_type_name,
                count(distinct pm.trans_id) as account_no_transactions_daily,
                sum(pm.amount) as account_daily_spending
            from dim_payment_transaction pm
                join dim_account a on pm.acc_id = a.acc_id
                join dim_account_type at on a.acc_type = at.type_id
            group by pm.transaction_date, a.cust_id, a.acc_id, at.type_nm
        ),
        cte_account_accum_revenue as (
            select transaction_date, cust_id, acc_id, account_daily_spending,
                sum(account_daily_spending) over (partition by cust_id, acc_id order by transaction_date) as account_accum_spending
            from cte_account_payment_summary
        ),
        cte_customer_avgerage_daily_spending as (
            select month(transaction_date) as month, a.cust_id, 
                avg(amount) as cust_avgerage_daily_spending
            from dim_payment_transaction pm join dim_account a on pm.acc_id = a.acc_id
            group by month(transaction_date), a.cust_id
        )
        
        select date_format(a1.transaction_date, 'yyyyMMdd') as date_key, a1.cust_id, a1.acc_id, a1.account_type_name,
                a1.account_no_transactions_daily,
                a1.account_daily_spending,
            a2.account_accum_spending,
            c1.cust_no_transaction_daily, c1.cust_daily_spending, c1.cust_daily_payment_type,
            c2.cust_accum_spending,
            c3.cust_avgerage_daily_spending
        from cte_account_payment_summary a1 
            join cte_account_accum_revenue a2 on a1.transaction_date = a2.transaction_date and a1.cust_id = a2.cust_id and a1.acc_id = a2.acc_id
            join cte_transaction_revenue c1 on a1.transaction_date = c1.transaction_date and a1.cust_id = c1.cust_id 
            join cte_cust_accum_revenue c2 on c1.cust_id = c2.cust_id 
            join cte_customer_avgerage_daily_spending c3 on c3.month = month(a1.transaction_date) and c3.cust_id = a1.cust_id
        order by a1.cust_id, a1.transaction_date
    """)

    spark.catalog.dropTempView("dim_payment_transaction")
    spark.catalog.dropTempView("dim_date")
    spark.catalog.dropTempView("dim_account")
    spark.catalog.dropTempView("dim_account_type")

    # export to S3
    output_name = 'kietl_fact_snapshot_daily_transaction'
    s3_golden_global_dest = f'{global_golden_root}/{output_name}'
    s3_golden_local_dest = f'{local_golden_root}/{output_name}'
    df_fact.write.mode('overwrite').parquet(s3_golden_local_dest)
    df_fact.write.mode('overwrite').parquet(s3_golden_global_dest)

    return df_fact

initEtl()
    
    

# def EtlCreateDateDimTable():
#     ''' the datetime dimension table represents the time aspect for all other tables'''
#     source_date_data = 'payment_transaction.csv'
#     s3_file_landing = f's3://{V.DATA_LANDING_BUCKET_NAME}/kietl/{table_s3_file}'
#     s3_saving_path = f's3://{V.DATA_LANDING_BUCKET_NAME}/kietl/golden_zone/dim_{table_s3_file}'
    
#     df = spark.read.option('header', 'true').option('delimiter', ',').csv(s3_file_landing)
    
#     df.select('transaction_time')
#         .withColumn()


# draft
# s3_file_landing = f's3://{V.DATA_LANDING_BUCKET_NAME}/kietl/account.csv'

# df = spark.read.option('header', 'true').option('delimiter', ',').csv(s3_file_landing)

# s3_saving_path = f's3://{V.DATA_LANDING_BUCKET_NAME}/golden_zone/demo_table'

# df.coalesce(1).write.mode('overwrite').parquet(s3_saving_path)
# job.commit()
