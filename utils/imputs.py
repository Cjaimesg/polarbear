import streamlit as st
from datetime import datetime, timedelta


def imput_query_parametrized_hash():
    qp_hash = st.text_input('Query Parameterized Hash')
    return qp_hash


def select_date():
    """
    Prompts the user to select a date using Streamlit's date_input widget.

    The default date presented to the user is five days before tomorrow's date.

    Returns:
        datetime.date: The date selected by the user.
    """
    # Calculate tomorrow's date
    today = datetime.now() + timedelta(days=1)

    # Calculate the default date (five days before tomorrow)
    date_default = today - timedelta(days=5)

    # Prompt the user to select a date, with the default date set
    date_selected = st.date_input("When do you apply clustering?", date_default)

    return date_selected


def select_date_range():
    """
    Prompts the user to select a date range using Streamlit's date_input widget.

    The default date range presented to the user starts 10 days before tomorrow's date
    and ends tomorrow. The minimum allowed date is 14 days before tomorrow's date,
    and the maximum allowed date is tomorrow.

    Returns:
        tuple: A tuple containing the start date and end date selected by the user.
    """
    # Calculate tomorrow's date
    today = datetime.now() + timedelta(days=1)

    # Calculate the default start date (10 days before tomorrow)
    start_time = today - timedelta(days=10)

    # Calculate the minimum allowed date (14 days before tomorrow)
    limit_date = today - timedelta(days=14)

    # Prompt the user to select a date range, with constraints and default values
    date_range = st.date_input(
        "Select your period",
        (start_time, today),
        start_time,
        today,
        format="YYYY-MM-DD",
        help=f'''Please select a date range for your analysis.
        The minimum range is 14 days ago ({limit_date.strftime('%Y-%m-%d')}) and the maximum is the current date.'''
    )

    return date_range


def select_warehouses(session):
    """
    Prompts the user to select warehouses from a list obtained via a SQL query.

    The function executes a SQL query to show available warehouses, converts the result
    to a list, and uses Streamlit's multiselect widget to allow the user to select multiple
    warehouses.

    Args:
        session (object): The database session object used to execute the SQL query.

    Returns:
        list: A list of selected warehouses.
    """
    # Execute SQL query to show available warehouses and select the "name" column
    whs = session.sql('SHOW WAREHOUSES').select('"name"').to_pandas()

    # Convert the resulting DataFrame to a list of warehouse names
    whs = whs.values.tolist()
    whs = [wh[0] for wh in whs]

    # Prompt the user to select warehouses using a multiselect widget
    whs_select = st.multiselect(
        label="Select your Warehouse",
        options=whs
    )

    return whs_select


def select_number_queries():
    """
    Prompts the user to select the number of queries to analyze using Streamlit's slider widget.

    The slider allows the user to select a number between 1 and 100, with a default value of 10.

    Returns:
        int: The number of queries selected by the user.
    """
    # Prompt the user to select the number of queries to analyze using a slider widget
    queries = st.slider("How many queries do you want to analyze?", 1, 100, 10)

    return queries


def select_duration_queries():
    """
    Prompts the user to select the minimum duration of the queries to analyze using Streamlit's slider widget.

    The slider allows the user to select a duration between 0 and 100, with a default value of 5.

    Returns:
        int: The minimum duration of the queries selected by the user.
    """
    # Prompt the user to select the minimum duration of the queries to analyze using a slider widget
    duration = st.slider(
        "What is the minimum duration of the queries you want to analyze?",
        min_value=0,
        max_value=100,
        value=5
    )

    return duration


def select_ratio_partition():
    """
    Prompts the user to select the minimum partition ratio using Streamlit's slider widget.

    The slider allows the user to select a ratio between 0.0 and 1.0, with a default value of 0.5.

    Returns:
        float: The minimum partition ratio selected by the user.
    """
    # Prompt the user to select the minimum partition ratio using a slider widget
    ratio = st.slider(
        "What is the minimum partition ratio?",
        min_value=0.0,
        max_value=1.0,
        value=0.5
    )

    return ratio


def select_ratio_row():
    """
    Prompts the user to select the maximum ratio of rows using Streamlit's slider widget.

    The slider allows the user to select a ratio between 0.0 and 1.0, with a default value of 0.5.

    Returns:
        float: The maximum ratio of rows selected by the user.
    """
    # Prompt the user to select the maximum ratio of rows using a slider widget
    ratio = st.slider(
        "What is the maximum ratio of rows?",
        min_value=0.0,
        max_value=1.0,
        value=0.5
    )

    return ratio
