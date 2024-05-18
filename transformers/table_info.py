import snowflake.snowpark.functions as F
import snowflake.snowpark.window as W
from snowflake.ml.modeling.framework import base
from snowflake import snowpark


class table_info(base.BaseTransformer):
    def __init__(
        self,
        *,
        df_tables: snowpark.DataFrame,
        session: snowpark.session
    ) -> None:

        self.input_cols = []
        self.output_cols = []
        self.session = session

        self.df_tables = self.set_df_tables(df_tables)

    def set_df_tables(self, df_tables: snowpark.DataFrame) -> snowpark.DataFrame:

        cols = ['TABLE_CATALOG', 'TABLE_SCHEMA',
                'TABLE_NAME', 'CLUSTERING_KEY',
                'ROW_COUNT', 'BYTES', 'CREATED']

        df_tables = df_tables.select(cols)\
            .filter(F.col('DELETED').is_null()) \
            .filter(F.col('TABLE_TYPE') == 'BASE TABLE') \
            .filter(F.col('IS_TRANSIENT') == 'NO') \
            .filter(F.col('IS_ICEBERG') == 'NO') \
            .filter(F.col('IS_DYNAMIC') == 'NO')

        window_by_creation = W.Window.partition_by(['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME'])\
            .order_by(F.col("CREATED").desc())

        df_tables = df_tables.withColumn("ROW_NUMBER", F.row_number().over(window_by_creation))
        df_tables = df_tables.filter(F.col("ROW_NUMBER") == 1)
        df_tables = df_tables.drop("ROW_NUMBER")

        return df_tables

    def transform(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:

        dataset = dataset.join(self.df_tables, on=['TABLE_CATALOG',
                                                   'TABLE_SCHEMA',
                                                   'TABLE_NAME'], how='inner')

        return dataset

    def _fit(self, dataset: snowpark.DataFrame) -> "table_info":
        return self

    def fit(self, dataset: snowpark.DataFrame) -> "table_info":
        return self
