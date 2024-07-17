import streamlit as st

from app.session import snowflake_session
from app.cluster_recomender import model_cluster_recomender
from app.cluster_analyzer import cluster_analyzer

from data_access.snowflake_data import data_access

st.set_page_config(
    initial_sidebar_state="expanded",
)


def find_cluster_recomender(m_cluster_recomender, df_query, df_tables):
    with st.form(key='base_form', border=False):
        m_cluster_recomender.collect_parameters_find_cluster()
        m_cluster_recomender.collect_more_parameters_find_cluster()
        submitted = st.form_submit_button("Submit")

    if submitted:
        df_cl_rec = m_cluster_recomender.execute_parameters_find_cluster(df_query, df_tables)
        df_cl_rec


def analyse_cluster(m_cluster_analyzer, df_query):
    with st.form(key='base_form', border=False):
        m_cluster_analyzer.collect_parameters()
        submitted = st.form_submit_button("Submit")

    if submitted:
        df_cl_ana = m_cluster_analyzer.execute_parameters_analize_cluster(df_query)
        st.header("Descriptive Analysis of Execution Time Before and After Optimization")
        m_cluster_analyzer.comparative_box_plot(df_cl_ana)
        m_cluster_analyzer.execution_time_history(df_cl_ana)
        m_cluster_analyzer.parametic_test(df_cl_ana)


def main():
    if 'params_collected' not in st.session_state:
        st.session_state.params_collected = False

    # Init snowflake session
    sf_session = snowflake_session("snowflake")
    session = sf_session.get_session()

    # Get Snowflae metadata
    sf_data = data_access(session)

    df_query = sf_data.get_table('snowflake.account_usage.QUERY_HISTORY')
    df_tables = sf_data.get_table('snowflake.account_usage.TABLES')

    st.title("Polar Bear :polar_bear:")
    st.header('Cluster Recomendator')

    page_names = ['Find Cluster Recommendations', 'Evaluate Cluster Recomendations']

    page = st.radio('Navigation', page_names, index=None)

    if page:
        st.write(f"**Mode {page} **")

    if page == 'Find Cluster Recommendations':
        m_cluster_recomender = model_cluster_recomender(session)
        find_cluster_recomender(m_cluster_recomender, df_query, df_tables)

    elif page == 'Evaluate Cluster Recomendations':
        m_cluster_analyzer = cluster_analyzer(session)
        analyse_cluster(m_cluster_analyzer, df_query)

    if st.button("Reset"):
        st.session_state.params_collected = False


if __name__ == "__main__":
    main()
