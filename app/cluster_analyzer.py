import streamlit as st
import pandas as pd
import plotly.express as px
import scipy.stats as stats

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
        st.header("Parameter Collection Section")

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

    def execution_time_history(self, df_cl_ana: pd.DataFrame):
        # Create the line plot

        fig = px.line(df_cl_ana, x="START_TIME", y="EXECUTION_TIME",
                      title='Evolution of query execution time before and after optimization', 
                      labels={'START_TIME': 'Date of Execution', 'EXECUTION_TIME': 'Execution Time (s)'})

        fig.update_layout(showlegend=False)

        st.plotly_chart(fig)

    def parametic_test(self, df_cl_ana: pd.DataFrame):
        # Split the data into two groups: Before and After

        st.header('Statistical Analysis of Execution Time Before and After Optimization')
        before = df_cl_ana[df_cl_ana['PERIOD'] == 'Before']['EXECUTION_TIME']
        after = df_cl_ana[df_cl_ana['PERIOD'] == 'After']['EXECUTION_TIME']

        mean_before = before.mean()
        mean_after = after.mean()

        # Perform the independent samples t-test
        t_stat, p_value = stats.ttest_ind(before, after)

        # Significance level
        alpha = 0.05

        # Interpret the results
        if p_value < alpha:
            result = 'Reject the null hypothesis: The means are significantly different.'
        else:
            result = 'Do not reject the null hypothesis: There is not enough evidence to say the means are significantly different.'

        # Report results
        st.write(f'Mean before: {mean_before:.4f} minutes')
        st.write(f'Mean after: {mean_after:.4f} minutes')
        st.write(f'T-statistic: {t_stat:.4f}')
        st.write(f'P-value: {p_value:.4f}')
        st.write(result)
