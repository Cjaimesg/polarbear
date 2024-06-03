import streamlit as st


class snowflake_session():
    def __init__(self, conn_name: str) -> None:
        self.conn_name = conn_name

    def get_session(self):
        conn = st.connection(self.conn_name)
        session = conn.session()
        return session
