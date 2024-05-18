import snowflake.snowpark.functions as F
from snowflake.ml.modeling.framework import base
from snowflake import snowpark


class top_offender_queries(base.BaseTransformer):
    def __init__(
        self,
        *,
        min_exce_time: float = 10,
    ) -> None:
        self.min_exce_time = min_exce_time

        self.input_cols = []
        self.output_cols = []

    def transform(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:

        # Selection of relevant columns and calculation of runtime in minutes.
        dataset = dataset.with_column('EXECUTION_TIME_MIN',
                                      F.round(F.col('EXECUTION_TIME')/1000/60, 2))
        dataset = dataset.select(['WAREHOUSE_NAME',
                                  'QUERY_TAG',
                                  'QUERY_ID',
                                  'EXECUTION_TIME_MIN',
                                  'QUERY_PARAMETERIZED_HASH'])

        # Grouping of data and calculation of statistics.
        dataset = dataset.group_by(['QUERY_PARAMETERIZED_HASH',
                                    'WAREHOUSE_NAME']).agg(
            F.max('QUERY_ID').alias('LAST_QUERY_ID'),
            F.max('QUERY_TAG').alias('QUERY_TAG'),
            F.sum('EXECUTION_TIME_MIN').alias('TOTAL_EXECUTION_TIME_MIN'),
            F.avg('EXECUTION_TIME_MIN').alias('AVG_EXECUTION_TIME_MIN'),
            F.count('EXECUTION_TIME_MIN').alias('COUNT')
        )

        # Final filter to include only queries with more than N minutes of total execution time and descending sorting.
        dataset = dataset.filter(F.col('TOTAL_EXECUTION_TIME_MIN') > self.min_exce_time)
        dataset = dataset.sort(F.col("TOTAL_EXECUTION_TIME_MIN"), ascending=False)

        return dataset

    def _fit(self, dataset: snowpark.DataFrame) -> "top_offender_queries":
        return self

    def fit(self, dataset: snowpark.DataFrame) -> "top_offender_queries":
        return self
