import streamlit as st
import mysql.connector
import pandas as pd
from datetime import datetime

# Define your MySQL database connection parameters
db_config_cilantro = {
    "host": "cliantro.cmbrsga0s9bx.me-central-1.rds.amazonaws.com",
    "port": 3306,
    "user": "cilantro",
    "password": "LSQiM7hoW7A3N7",
    "database": "cilantrodb"
}

# Function to fetch data
def fetch_data_rfm(start_date, end_date, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    
    query = """
    WITH UserOrders AS (
        SELECT 
            tbl_user.id AS user_id,
            tbl_orders.id AS order_id,
            tbl_orders.confirm_datetime AS order_date,
            tbl_orders.item_total_amount AS transaction_value,
            tbl_user.first_name AS first_name
        FROM cilantrodb.tbl_orders
        LEFT JOIN cilantrodb.tbl_user ON cilantrodb.tbl_orders.user_id = cilantrodb.tbl_user.id
        WHERE tbl_orders.confirm_datetime BETWEEN %s AND %s
    ),
    AggregatedUserOrders AS (
        SELECT
            user_id,
            MAX(order_date) AS max_order_date,
            COUNT(order_id) AS number_of_orders,
            AVG(transaction_value) AS average_transaction_value,
            DATEDIFF(NOW(), MAX(order_date)) AS days_since_last_order,
            COUNT(order_id) AS frequency,
            SUM(transaction_value) AS monetary_value,
            first_name
        FROM UserOrders
        GROUP BY user_id, first_name
    ),
    RankedUserOrders AS (
        SELECT
            user_id,
            max_order_date,
            number_of_orders,
            average_transaction_value,
            days_since_last_order,
            NTILE(9) OVER (ORDER BY days_since_last_order) AS recency_rank,
            NTILE(9) OVER (ORDER BY frequency DESC) AS frequency_rank,
            NTILE(9) OVER (ORDER BY monetary_value DESC) AS monetary_value_rank,
            first_name
        FROM AggregatedUserOrders
    ),
    UserTopBranch AS (
        SELECT 
            user_id, 
            tbl_vendor.name AS top_branch,
            RANK() OVER (PARTITION BY tbl_orders.user_id ORDER BY COUNT(tbl_orders.id) DESC) AS branch_rank
        FROM cilantrodb.tbl_orders
        LEFT JOIN cilantrodb.tbl_vendor ON tbl_orders.vendor_id = tbl_vendor.id
        GROUP BY user_id, tbl_vendor.name
    )
    SELECT
        r.user_id,
        r.max_order_date,
        r.number_of_orders,
        r.average_transaction_value,
        r.days_since_last_order,
        r.recency_rank,
        r.frequency_rank,
        r.monetary_value_rank,
        r.first_name,
        t.top_branch
    FROM RankedUserOrders r
    LEFT JOIN UserTopBranch t ON r.user_id = t.user_id AND t.branch_rank = 1;
    """

    cursor.execute(query, (start_date, end_date))
    data = cursor.fetchall()

    cursor.close()
    connection.close()

    return data

# Function to categorize ranks
def categorize_rank(rank):
    if rank >= 7:
        return 'low'
    elif rank >= 4:
        return 'moderate'
    else:
        return 'high'

# Define the Streamlit app
def main():
    st.title("Better Segmentanator")

    # Date range filter
    start_date = st.date_input('Start date', datetime(2023, 5, 20))
    end_date = st.date_input('End date', datetime.today())

    if start_date > end_date:
        st.error('Error: End date must fall after start date.')
    else:
        # Fetch data
        response = fetch_data_rfm(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), db_config_cilantro)

        # Create DataFrame
        columns = ['userid', 'last_order_date', 'number_of_orders', 'average_transaction_value', 'days_since_last_order', 'recency_rank', 'frequency_rank', 'monetary_value_rank', 'first_name', 'top_branch']
        df = pd.DataFrame(response, columns=columns)

        # Add rank categories
        df['recency_category'] = df['recency_rank'].apply(categorize_rank)
        df['frequency_category'] = df['frequency_rank'].apply(categorize_rank)
        df['monetary_category'] = df['monetary_value_rank'].apply(categorize_rank)

        # Filters for rank categories
        recency_filter = st.multiselect('Recency category', options=['high', 'moderate', 'low'], default=['high', 'moderate', 'low'])
        frequency_filter = st.multiselect('Frequency category', options=['high', 'moderate', 'low'], default=['high', 'moderate', 'low'])
        monetary_filter = st.multiselect('Monetary category', options=['high', 'moderate', 'low'], default=['high', 'moderate', 'low'])
        days_since_last_order_min = st.number_input('Minimum Days Since Last Order', value=0)
        days_since_last_order_max = st.number_input('Maximum Days Since Last Order', value=df['days_since_last_order'].max())

        # Filter for branches
        branches = df['top_branch'].unique()
        selected_branches = st.multiselect('Branches', options=branches, default=branches)

        filtered_df = df[
            (df['recency_category'].isin(recency_filter)) &
            (df['frequency_category'].isin(frequency_filter)) &
            (df['monetary_category'].isin(monetary_filter)) &
            (df['days_since_last_order'] >= days_since_last_order_min) &
            (df['days_since_last_order'] <= days_since_last_order_max) &
            (df['top_branch'].isin(selected_branches))
        ]

        st.dataframe(filtered_df)

if __name__ == '__main__':
    main()
