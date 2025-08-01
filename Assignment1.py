import sqlite3
import pandas as pd
import os

DB_FILE = 'sales_data.db'

def sql_solution():
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        query = """
        SELECT
            c.customer_id,
            c.age,
            i.item_name,
            CAST(SUM(o.quantity) AS INTEGER) AS total_quantity
        FROM
            Customer c
        INNER JOIN
            Sales s ON c.customer_id = s.customer_id
        INNER JOIN
            Orders o ON s.sales_id = o.sales_id
        INNER JOIN
            Items i ON o.item_id = i.item_id
        WHERE
            c.age BETWEEN 18 AND 35
            AND i.item_name IN ('x', 'y', 'z')
        GROUP BY
            c.customer_id, c.age, i.item_name
        HAVING
            SUM(o.quantity) > 0
        ORDER BY
            c.customer_id, i.item_name;
        """
        result_df = pd.read_sql_query(query, conn)
        return result_df
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def pandas_solution():
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        customers = pd.read_sql_query("SELECT * FROM Customer", conn)
        sales = pd.read_sql_query("SELECT * FROM Sales", conn)
        orders = pd.read_sql_query("SELECT * FROM Orders", conn)
        items = pd.read_sql_query("SELECT * FROM Items", conn)

        df_merged = pd.merge(customers, sales, on='customer_id', how='inner')
        df_merged = pd.merge(df_merged, orders, on='sales_id', how='inner')
        df_merged = pd.merge(df_merged, items, on='item_id', how='inner')

        age_filter = (df_merged['age'] >= 18) & (df_merged['age'] <= 35)
        item_filter = df_merged['item_name'].isin(['x', 'y', 'z'])
        df_filtered = df_merged[age_filter & item_filter]

        df_result = df_filtered.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()

        df_result.rename(columns={'quantity': 'Quantity', 'customer_id': 'Customer', 'item_name': 'Item', 'age': 'Age'}, inplace=True)
        df_result['Quantity'] = df_result['Quantity'].astype(int)
        df_result = df_result[df_result['Quantity'] > 0]

        df_result = df_result.sort_values(by=['Customer', 'Item']).reset_index(drop=True)
        return df_result
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def main():
    print("Running SQL solution...")
    sql_result = sql_solution()
    if not sql_result.empty:
        print("SQL Solution Output:")
        print(sql_result)
    
    print("\nRunning Pandas solution...")
    pandas_result = pandas_solution()
    if not pandas_result.empty:
        print("Pandas Solution Output:")
        print(pandas_result)

    final_output = sql_result
    
    output_filename = "output.csv"
   
    if not final_output.empty:
        final_output.to_csv(
            output_filename,
            sep=';',
            index=False,
            header=['Customer', 'Age', 'Item', 'Quantity']
        )
        print(f"\nResults successfully saved to {os.path.abspath(output_filename)}")

if __name__ == "__main__":
    main()