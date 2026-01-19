import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import ollama
    return alt, mo, ollama


@app.cell
def _(mo):
    orders = mo.sql(
        f"""
        select * from read_csv('data/orders.csv')
        """,
        output=False
    )
    return (orders,)


@app.cell
def _(mo):
    customers = mo.sql(
        f"""
        select * from read_csv('data/customers.csv')
        """,
        output=False
    )
    return (customers,)


@app.cell
def _(mo):
    products = mo.sql(
        f"""
        select * from read_csv('data/products.csv')
        """,
        output=False
    )
    return (products,)


@app.cell
def _(customers, mo, orders, products):
    order_details = mo.sql(
        f"""
        SELECT
            customers.name AS customer,
            products.name AS product,
            orders.quantity,
            orders.order_date
        FROM orders
        INNER JOIN customers
            on orders.customer_id = customers.customer_id
        INNER JOIN products
            on orders.product_id = products.product_id
        """
    )
    return (order_details,)


@app.cell
def _(mo, order_details):
    quantity_by_product = mo.sql(
        f"""
        SELECT
            product,
            SUM(quantity) AS total_quantity
        FROM order_details
        GROUP BY product
        """
    )
    return (quantity_by_product,)


@app.cell
def _(alt, quantity_by_product):
    bar_chart = alt.Chart(quantity_by_product).mark_bar().encode(
        x=alt.X('product:N', title='Product'),
        y=alt.Y('total_quantity:Q', title='Total Quantity'),
        tooltip=['product', 'total_quantity']
    ).properties(
        title='Quantity Sold by Product',
        width=400,
        height=300
    )
    bar_chart
    return


@app.cell
def _(mo, order_details):
    orders_by_date = mo.sql(
        f"""
        SELECT
            order_date,
            SUM(quantity) AS total_quantity
        FROM order_details
        GROUP BY order_date
        ORDER BY order_date
        """
    )
    return (orders_by_date,)


@app.cell
def _(alt, orders_by_date):
    line_chart = alt.Chart(orders_by_date).mark_line(point=True).encode(
        x=alt.X('order_date:T', title='Order Date'),
        y=alt.Y('total_quantity:Q', title='Total Quantity'),
        tooltip=['order_date', 'total_quantity']
    ).properties(
        title='Orders Over Time',
        width=500,
        height=300
    )
    line_chart
    return


@app.cell
def _(mo):
    customers1 = mo.sql(
        f"""
        select * from read_csv('data/customers1.csv')
        """,
        output=False
    )
    return (customers1,)


@app.cell
def _(mo):
    customers2 = mo.sql(
        f"""
        select * from read_csv('data/customers2.csv')
        """,
        output=False
    )
    return (customers2,)


@app.cell
def _(customers1, customers2, ollama):
    names1 = customers1['name'].to_list()
    names2 = customers2['name'].to_list()

    prompt = f"""You are a data cleaning assistant. Compare these two lists of customer names and identify likely matches where the same person appears with slightly different names.

    List 1:
    {chr(10).join(names1)}

    List 2:
    {chr(10).join(names2)}

    Return a JSON array of objects with "original_name" and "resolved_name" keys.
    For names in List 1 that match a name in List 2, use the List 2 version as resolved_name.
    For names in List 1 with no match, use the original name as resolved_name.
    Include ALL names from List 1.

    Example output:
    [
      {{"original_name": "Johnny Smith", "resolved_name": "John Smith"}},
      {{"original_name": "Jane Doe", "resolved_name": "Jane Doe"}}
    ]

    Return ONLY the JSON array, no other text.
    """

    response = ollama.chat(
        model='gemma3:4b',
        messages=[{'role': 'user', 'content': prompt}]
    )

    name_matches = response['message']['content']
    name_matches
    return (name_matches,)


@app.cell
def _(name_matches):
    import json
    import polars as pl

    # Parse the JSON response
    cleaned = name_matches.strip()
    if cleaned.startswith('```'):
        cleaned = cleaned.split('\n', 1)[1]
        cleaned = cleaned.rsplit('```', 1)[0]

    mappings = json.loads(cleaned)

    # Create DataFrame and save to CSV
    resolved_df = pl.DataFrame(mappings)
    resolved_df.write_csv('data/customers_resolved.csv')
    resolved_df
    return


if __name__ == "__main__":
    app.run()
