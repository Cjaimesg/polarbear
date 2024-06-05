import streamlit as st
import pandas as pd
import plotly.express as px

from pipeline.PipelineV2 import PipelineV2
from utils.imputs import select_date
from utils.imputs import select_date_range
from utils.imputs import select_warehouses
from utils.imputs import imput_query_parametrized_hash

from transformers.preprocessor import preprocessor
from transformers.execution_time_history import execution_time_history


class cluster_analyzer():
    def __init__(self, session) -> None:
        self.session = session

    def collect_parameters(self):
        st.header("Sección de Recolección de Parámetros")

        date_range = select_date_range()
        date_split = select_date()
        qp_hash = imput_query_parametrized_hash()
        whs_selected = select_warehouses(self.session)

        # Botón para indicar que se han terminado de configurar los parámetros
        st.session_state.start_time = date_range[0]
        st.session_state.end_time = date_range[1]
        st.session_state.date_split = date_split
        st.session_state.qp_hash = qp_hash
        st.session_state.whs_selected = whs_selected
        st.session_state.params_collected = True

    def execute_parameters_analize_cluster(self, df_query):
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
                    "EXECUTION_TIME_HISTORY",
                    execution_time_history(
                        qp_hash=st.session_state.qp_hash,
                        date_split=st.session_state.date_split
                    )
                )
            ]
        )

        pipeline.fit(df_query)
        df_cl_ana = pipeline.transform(df_query)
        df_cl_ana = df_cl_ana.cache_result()
        df_cl_ana = df_cl_ana.select(['EXECUTION_TIME', 'START_TIME', 'PERIOD'])
        df_cl_ana = df_cl_ana.to_pandas()

        df_cl_ana['EXECUTION_TIME'] = df_cl_ana['EXECUTION_TIME'] / 1000

        return df_cl_ana

    def comparative_box_plot(self, df_cl_ana: pd.DataFrame):
        # Create the boxplot
        fig = px.box(df_cl_ana, x='PERIOD', y='EXECUTION_TIME',
                     title='Comparison of query execution time before and after optimization',
                     labels={'PERIOD': 'Period', 'EXECUTION_TIME': 'Execution Time (s)'})

        fig.update_layout(showlegend=False)

        st.plotly_chart(fig)
