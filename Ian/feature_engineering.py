import duckdb
import pandas as pd
from pandas_gbq import to_gbq

def run_feature_engineering_script():
    try:
        con = duckdb.connect("/home/chuhao/dsai_sctp/br_e_commerce/duckdb/br_temp.db")
        fct_orders = con.execute("SELECT * FROM olist.fct_orders").fetchdf()
        dim_customers = con.execute("SELECT * FROM olist.dim_customers").fetchdf()
        dim_items = con.execute("SELECT * FROM olist.dim_items").fetchdf()

        df_delivery = fct_orders.copy()
        df_delivery['actual_delivery_time'] = df_delivery['order_delivered_customer_date'] - df_delivery['order_purchase_timestamp']
        df_delivery['estimated_delivery_time'] = df_delivery['order_estimated_delivery_date'] - df_delivery['order_purchase_timestamp']
        df_delivery['actual_delivery_time_minutes'] = df_delivery['actual_delivery_time'].dt.total_seconds() / 60
        df_delivery['estimated_delivery_time_minutes'] = df_delivery['estimated_delivery_time'].dt.total_seconds() / 60

        to_gbq(
            df_delivery,
            destination_table='olist_brazilian_ecommerce_DS.DS_fct_orders_delivery',
            project_id='projectm2-aiess',
            if_exists='replace',
        )

        orders_customers = pd.merge(fct_orders, dim_customers, how='left', on='customer_sid')
        orders_full = pd.merge(orders_customers, dim_items[['item_sid', 'price']], how='left', on='item_sid')
        orders_full['profit'] = orders_full['TOTAL_PAYMENT'] - orders_full['price']

        to_gbq(
            orders_full,
            destination_table='olist_brazilian_ecommerce_DS.DS_orders_full_profits',
            project_id='projectm2-aiess',
            if_exists='replace',
        )

        return "Feature engineering completed successfully"
    except Exception as e:
        print(f"feature_engineering.py failed with error: {e}")
        raise



if __name__ == "__main__":
    run_feature_engineering_script()