import pytest
from lib.ConfigReader import get_app_config
from lib.DataReader import read_customers, read_orders
from lib.DataManupulation import filter_closed_orders, join_orders_customers, counts_orders_state, filter_generic_order_status
from conftest import spark

@pytest.mark.transformation()
def test_read_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

@pytest.mark.transformation()
def test_read_orders(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_filtered_data(spark):
    ord_df = read_orders(spark,"LOCAL")
    filter_count = filter_closed_orders(ord_df).count()
    assert filter_count == 7556

@pytest.mark.transformation()
def test_count_orders_state(spark, expected_results):
    ord_df = read_orders(spark,"LOCAL")
    ord_df = filter_closed_orders(ord_df)
    customers_df = read_customers(spark,"LOCAL")
    joined_df = join_orders_customers(ord_df, customers_df)
    final_df = counts_orders_state(joined_df)
    assert final_df.collect() == expected_results.collect()



@pytest.mark.parametrize(
    "status,count",
    [("CLOSED",7556),
     ("PENDING_PAYMENT", 15030),
     ("COMPLETE", 22900)]

)

def test_generic_filtered_data(spark,status,count):
    ord_df = read_orders(spark,"LOCAL")
    filter_count = filter_generic_order_status(ord_df,status).count()
    assert filter_count == count












    



