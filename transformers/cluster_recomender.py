import snowflake.snowpark.functions as F
from snowflake.ml.modeling.framework import base
from snowflake import snowpark

import pandas as pd
import re


class cluster_recomender(base.BaseTransformer):
    def __init__(
        self,
        *,
        ratio_row: float = 0.5,
        ratio_partitions_scanned: float = 0.5,
        session: snowpark.session
    ) -> None:

        self.set_ratio_row(ratio_row)
        self.set_ratio_partitions_scanned(ratio_partitions_scanned)
        self.session = session

        self.input_cols = []
        self.output_cols = []

    def set_ratio_row(self, ratio_row: float):
        if not isinstance(ratio_row, float):
            raise TypeError("ratio_row must be a float")
        if ratio_row > 1:
            raise ValueError("ratio_row cannot be greater than 1")
        if ratio_row < 0:
            raise ValueError("ratio_row cannot be less than 0")
        self.ratio_row = ratio_row

    def set_ratio_partitions_scanned(self, ratio_partitions_scanned: float):
        if not isinstance(ratio_partitions_scanned, float):
            raise TypeError("ratio_partitions_scanned must be a float")
        if ratio_partitions_scanned > 1:
            raise ValueError("ratio_partitions_scanned cannot be greater than 1")
        if ratio_partitions_scanned < 0:
            raise ValueError("ratio_partitions_scanned cannot be less than 0")
        self.ratio_partitions_scanned = ratio_partitions_scanned

    def transform(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:
        dataset = dataset.cache_result()
        dataset = dataset.withColumn('RATIO_ROW', F.col('OUTPUT_ROWS')/F.col('ROW_COUNT'))

        """
        dataset = dataset.filter(F.col('RATIO_ROW') < self.ratio_row) \
            .filter(F.col('RATIO_PARTITIONS_SCANNED') > self.ratio_partitions_scanned)
        """

        tables = dataset.filter(F.col('COLUMN_AS_FILTER'))\
            .select(['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COL', 'COLUMN_AS_FILTER'])

        cardinality_info = self.get_cardinality(tables)
        col_info = self.get_col_info(tables)
        col_info = cardinality_info.join(col_info, on=['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COL'],
                                         how='inner')

        col_info = col_info.withColumn("CLUSTER_RECOMENDATION",
            F.when(F.col("TYPE") == 'StringType', F.lit(F.concat(F.lit("SUBSTRING("), F.col("COL"), F.lit(', 0, '), F.lit(F.col('ORDER') - 4), F.lit(")")))) \
            .when(F.col("TYPE") == 'LongType', F.lit(F.concat(F.lit("TRUNCATE("), F.col("COL"), F.lit(', '), F.lit(-F.col('ORDER') + 4), F.lit(")")))) \
            .when(F.col("TYPE") == 'DoubleType', F.lit(F.concat(F.lit("TRUNCATE("), F.col("COL"), F.lit(', '), F.lit(-F.col('ORDER') + 4), F.lit(")")))) \
            .when(F.col("TYPE") == 'IntegerType', F.lit(F.concat(F.lit("TRUNCATE("), F.col("COL"), F.lit(', '), F.lit(-F.col('ORDER') + 4), F.lit(")")))) \
            .when(F.col("TYPE") == 'FloatType', F.lit(F.concat(F.lit("TRUNCATE("), F.col("COL"), F.lit(', '), F.lit(-F.col('ORDER') + 4), F.lit(")")))) \
            .when(F.col("TYPE") == 'DecimalType', F.lit(F.concat(F.lit("TRUNCATE("), F.col("COL"), F.lit(', '), F.lit(-F.col('ORDER') + 4), F.lit(")")))) \
            .when(F.col("TYPE") == 'BooleanType', F.col("COL")) \
            .when(F.col("TYPE") == 'TimestampType', F.lit(F.concat(F.lit("DATE("), F.col("COL"), F.lit(")")))) \
            .when(F.col("TYPE") == 'DateType', F.lit(F.concat(F.lit("DATE("), F.col("COL"), F.lit(")")))) \
            .otherwise(F.lit(" -- Case Not Implemented")))

        col_info = col_info.drop_duplicates()

        dataset = dataset.join(col_info, on=['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COL'], how='left')

        return dataset

    def get_cardinality(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:
        tables_df = dataset.to_pandas()
        total_tablas = len(tables_df)

        if total_tablas > 100:
            raise ValueError('Many tables to be analyzed by get_cardinality')

        cardinality_list = []

        for index, i_row in tables_df.iterrows():
            cardinality = self.get_one_cardinality(
                db=i_row['TABLE_CATALOG'],
                sch=i_row['TABLE_SCHEMA'],
                table=i_row['TABLE_NAME'],
                column=i_row['COL'],
            )

            cardinality_list.append(cardinality)

        cardinality_info = cardinality_list[0]
        for i_col_info in cardinality_list[1:]:
            cardinality_info = cardinality_info.union_all(i_col_info)

        return cardinality_info

    def get_one_cardinality(self,
                            db: str,
                            sch: str,
                            table: str,
                            column: str) -> snowpark.DataFrame:

        tabla = self.session.table(f'{db}.{sch}.{table}')

        cardinality = tabla.select(
            F.lit(db).alias('TABLE_CATALOG'),
            F.lit(sch).alias('TABLE_SCHEMA'),
            F.lit(table).alias('TABLE_NAME'),
            F.lit(column).alias('COL'),
            F.approx_count_distinct(column).alias("CARDINALITY"),
            F.floor(F.log(10, F.col("CARDINALITY"))).alias("ORDER"))

        return cardinality

    def get_one_col_info(self,
                         db: str,
                         sch: str,
                         table: str,
                         column: str) -> snowpark.DataFrame:

        fields = self.session.table(f'{db}.{sch}.{table}').schema.fields

        field_types = []
        for field in fields:
            col = field.column_identifier.quoted_name
            col = col.replace('"', '')

            normaliced_type = re.sub(r'\(.*?\)', '', str(field.datatype))

            field_type = {
                'TABLE_CATALOG': db,
                'TABLE_SCHEMA': sch,
                'TABLE_NAME': table,
                'COL': col,
                'TYPE': normaliced_type}
            field_types.append(field_type)

        field_types = pd.DataFrame.from_dict(field_types)
        col_info = self.session.create_dataframe(field_types)

        return col_info

    def get_col_info(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:
        tables_df = dataset.to_pandas()
        total_tablas = len(tables_df)

        if total_tablas > 100:
            raise ValueError('Many tables to be analyzed by get_cardinality')

        col_info_list = []
        for index, i_row in tables_df.iterrows():
            col = self.get_one_col_info(
                db=i_row['TABLE_CATALOG'],
                sch=i_row['TABLE_SCHEMA'],
                table=i_row['TABLE_NAME'],
                column=i_row['COL'],
            )

            col_info_list.append(col)

        col_info = col_info_list[0]
        for i_col_info in col_info_list[1:]:
            col_info = col_info.union_all(i_col_info)

        return col_info

    def _fit(self, dataset: snowpark.DataFrame) -> "cluster_recomender":
        return self

    def fit(self, dataset: snowpark.DataFrame) -> "cluster_recomender":
        return self
