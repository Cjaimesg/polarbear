import streamlit as st
from datetime import datetime, timedelta


def select_date_range():
    today = datetime.now()
    start_time = today - timedelta(days=10)
    limit_date = today - timedelta(days=14)

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
    whs = session.sql('SHOW WAREHOUSES').select('"name"').to_pandas()
    whs = whs.values.tolist()

    whs = [wh[0] for wh in whs]

    whs_select = st.multiselect(
        label="Select your Warehouse",
        options=whs
    )

    return whs_select


def select_number_queries():
    queries = st.slider("How many queries do you want to analyze?", 1, 100, 10)
    return queries


def select_duration_queries():
    duration = st.slider(
        "What is the minimum duration of the queries you want to analyze?",
        min_value=0,
        max_value=100,
        value=5)
    return duration


def select_ratio_partition():
    ratio = st.slider(
        "What is the minimum partition ratio?",
        min_value=0.0,
        max_value=1.0,
        value=0.5)
    return ratio


def select_ratio_row():
    ratio = st.slider(
        "What is the maximum ratio of rows?",
        min_value=0.0,
        max_value=1.0,
        value=0.5)
    return ratio
