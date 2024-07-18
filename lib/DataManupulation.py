from pyspark.sql.functions import *


def filter_closed_orders(df):
    return df.filter("order_status = 'CLOSED'")


def join_orders_customers(orders_df,customers_df):
    return orders_df.join(customers_df,orders_df.customer_id == customers_df.customer_id,'inner')



def counts_orders_state(joined_df):
    return joined_df.groupBy('state').count()


def filter_generic_order_status(df,status):
    return df.filter("order_status = '{}'".format(status))









