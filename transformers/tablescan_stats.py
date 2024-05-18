import snowflake.snowpark.functions as F
from snowflake.ml.modeling.framework import base
from snowflake.snowpark.types import StringType
from snowflake import snowpark
from typing import Iterable


class tablescan_stats(base.BaseTransformer):
    def __init__(
        self,
        *,
        session: snowpark.session,
        n_queries: int = 10
    ) -> None:

        self.session = session
        self.n_queries = n_queries
        self.input_cols = []
        self.output_cols = []

    def val_reserved_columns(self, dataset: snowpark.DataFrame) -> None:
        pass

    def transform(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:

        self.val_reserved_columns(dataset)

        dataset = dataset.sort(F.col('TOTAL_EXECUTION_TIME_MIN'), ascending=False)
        dataset = dataset.limit(self.n_queries)
        dataset = dataset.cache_result()

        # Obtener las columnas que se usan como filtro en cada tabla del query
        unique_queries = dataset.select('LAST_QUERY_ID').to_pandas()['LAST_QUERY_ID'].to_list()

        df_stats = self.collect_tablescan_stats_for_queries(unique_queries)

        df_stats = df_stats.select([
            'QUERY_ID',
            'TE_OVERALL_PERCENTAGE',
            'PARTITIONS_SCANNED',
            'PARTITIONS_TOTAL',
            'OUTPUT_ROWS',
            'TABLE_NAME',
            'FILTER_CONDITION',
            F.flatten(F.col('COLUMNS'), outer=True)])

        df_stats = df_stats.drop(['COLUMNS', 'SEQ', 'KEY', 'PATH', 'INDEX', 'THIS'])

        df_stats = df_stats.with_column_renamed(existing='QUERY_ID', new='LAST_QUERY_ID')
        df_stats = df_stats.with_column_renamed(new='COL', existing='VALUE')
        df_stats = df_stats.with_column('COL', F.col('COL').cast(StringType()))

        df_stats = df_stats.withColumn("COLUMN_AS_FILTER", F.col("FILTER_CONDITION").contains(F.col('COL')))
        df_stats = df_stats.filter(F.col('COLUMN_AS_FILTER'))

        # Unir las consultas sobre el rendimiento de las particiones y las metricas de ejecucion
        dataset = dataset.join(df_stats, on='LAST_QUERY_ID')

        dataset = dataset.with_column('SCANNING_TIME_MIN',
                                      F.round(F.col('TOTAL_EXECUTION_TIME_MIN') * F.col('TE_OVERALL_PERCENTAGE'), 2))

        dataset = dataset.with_column('RATIO_PARTITIONS_SCANNED',
                                      F.round(F.col('PARTITIONS_SCANNED') / F.col('PARTITIONS_TOTAL'), 1))

        # Separar los nombres de la tabla en base de datos, schema y tabla
        dataset = dataset.withColumn("SPLIT_NAME", F.split(F.col("TABLE_NAME"), F.lit(".")))
        dataset = dataset.withColumn("TABLE_CATALOG", F.cast(F.col("SPLIT_NAME").getItem(0), StringType()))
        dataset = dataset.withColumn("TABLE_SCHEMA", F.cast(F.col("SPLIT_NAME").getItem(1), StringType()))
        dataset = dataset.withColumn("TABLE_NAME", F.cast(F.col("SPLIT_NAME").getItem(2), StringType()))

        dataset = dataset.drop(['SPLIT_NAME'])

        return dataset

    def collect_tablescan_stats_for_queries(self, unique_queries: Iterable[str]) -> snowpark.DataFrame:
        stats = []
        for query_id in unique_queries:
            df_ts = self.get_query_tablescan_stats(query_id)
            stats.append(df_ts)

        df_stats = stats[0]
        for df in stats[1:]:
            df_stats = df_stats.union_all(df)

        return df_stats

    def get_query_tablescan_stats(self, query_id: str) -> snowpark.DataFrame:
        df_opstat = self.session.sql(f"SELECT * FROM table(get_query_operator_stats('{query_id}'))")
        df_opstat = df_opstat.cache_result()

        df_ts = df_opstat\
            .filter(F.col('OPERATOR_TYPE') == 'TableScan')\
            .with_column('TABLE_NAME', F.sql_expr('OPERATOR_ATTRIBUTES:table_name::STRING'))\
            .with_column('COLUMNS', F.sql_expr('OPERATOR_ATTRIBUTES:columns::VARIANT'))\
            .with_column('PARTITIONS_SCANNED', F.sql_expr('OPERATOR_STATISTICS:pruning:partitions_scanned::INT'))\
            .with_column('PARTITIONS_TOTAL', F.sql_expr('OPERATOR_STATISTICS:pruning:partitions_total::INT'))\
            .with_column('OUTPUT_ROWS', F.sql_expr('OPERATOR_STATISTICS:output_rows::INT'))\
            .with_column('PARENT_OPERATORS', F.sql_expr('PARENT_OPERATORS[0]'))\
            .with_column('TE_OVERALL_PERCENTAGE', F.sql_expr('EXECUTION_TIME_BREAKDOWN:overall_percentage'))\
            .select('QUERY_ID', 'STEP_ID', 'OPERATOR_ID',
                    'PARENT_OPERATORS', 'OPERATOR_TYPE',
                    'PARTITIONS_SCANNED', 'PARTITIONS_TOTAL',
                    'OUTPUT_ROWS',
                    'TABLE_NAME', 'COLUMNS',
                    'TE_OVERALL_PERCENTAGE')

        df_filter = df_opstat\
            .filter(F.col('OPERATOR_TYPE') == 'Filter')\
            .with_column('FILTER_CONDITION', F.sql_expr('OPERATOR_ATTRIBUTES:filter_condition::STRING'))\
            .select(['STEP_ID', 'OPERATOR_ID', 'FILTER_CONDITION'])

        df_ts = df_ts.join(df_filter,
                           (df_ts['PARENT_OPERATORS'] == df_filter['OPERATOR_ID'])
                           & (df_ts['STEP_ID'] == df_filter['STEP_ID'])
                           )

        return df_ts

    def _fit(self, dataset: snowpark.DataFrame) -> "tablescan_stats":
        return self

    def fit(self, dataset: snowpark.DataFrame) -> "tablescan_stats":
        return self
