import streamlit as st

from pipeline.PipelineV2 import PipelineV2
from utils.imputs import select_date_range
from utils.imputs import select_warehouses
from utils.imputs import select_number_queries
from utils.imputs import select_duration_queries
from utils.imputs import select_ratio_partition
from utils.imputs import select_ratio_row

from transformers.cluster_recomender import cluster_recomender
from transformers.preprocessor import preprocessor
from transformers.table_info import table_info
from transformers.tablescan_stats import tablescan_stats
from transformers.top_offender_queries import top_offender_queries


class model_cluster_recomender():
    def __init__(self, session) -> None:
        self.session = session

    def collect_parameters_find_cluster(self):
        st.header("Parameter Collection Section")

        n_queries = 10
        duration = 10
        r_partition = 0.5
        r_row = 0.5

        date_range = select_date_range()
        whs_selected = select_warehouses(self.session)

        # Botón para indicar que se han terminado de configurar los parámetros
        st.session_state.start_time = date_range[0]
        st.session_state.end_time = date_range[1]
        st.session_state.whs_selected = whs_selected
        st.session_state.n_queries = n_queries
        st.session_state.duration = duration
        st.session_state.r_partition = r_partition
        st.session_state.r_row = r_row
        st.session_state.params_collected = True

    def collect_more_parameters_find_cluster(self):
        n_queries = select_number_queries()
        duration = select_duration_queries()
        r_partition = select_ratio_partition()
        r_row = select_ratio_row()

        # Botón para indicar que se han terminado de configurar los parámetros
        st.session_state.n_queries = n_queries
        st.session_state.duration = duration
        st.session_state.r_partition = r_partition
        st.session_state.r_row = r_row
        st.session_state.params_collected = True

    def execute_parameters_find_cluster(self, df_query, df_tables):
        pipeline = PipelineV2(
            steps=[
                (
                    "PREPROCESSOR",
                    preprocessor(
                        start_time=st.session_state.start_time,
                        end_time=st.session_state.end_time,
                        wh_name=st.session_state.whs_selected,
                    )
                ),
                (
                    "TOP_OFFENDERS",
                    top_offender_queries(
                        min_exce_time=st.session_state.duration
                    )
                ),
                (
                    "QUERY_TABLESCAN_STATS",
                    tablescan_stats(
                        session=self.session,
                        n_queries=st.session_state.n_queries
                    )
                ),
                (
                    "TABLE_INFO",
                    table_info(
                        session=self.session,
                        df_tables=df_tables
                    )
                ),
                (
                    "CLUSTER_RECOMENDER",
                    cluster_recomender(session=self.session)
                )
            ]
        )

        pipeline.fit(df_query)
        df_cl_rec = pipeline.transform(df_query)
        df_cl_rec = df_cl_rec.cache_result()
        df_cl_rec = df_cl_rec.select([
            'QUERY_PARAMETERIZED_HASH',
            'TABLE_CATALOG',
            'TABLE_SCHEMA',
            'TABLE_NAME',
            'WAREHOUSE_NAME',
            'TOTAL_EXECUTION_TIME_MIN',
            'COUNT',
            'CLUSTER_RECOMENDATION',
            'TYPE'])
        df_cl_rec = df_cl_rec.to_pandas()

        return df_cl_rec
