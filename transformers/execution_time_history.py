import snowflake.snowpark.functions as F
from snowflake.ml.modeling.framework import base
from snowflake import snowpark


class execution_time_history(base.BaseTransformer):
    def __init__(
        self,
        *,
        qp_hash: str,
        date_split,
    ) -> None:

        self.input_cols = []
        self.output_cols = []
        self.set_qp_hahs(qp_hash)
        self.date_split = date_split

    def transform(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:

        # Filters applied to the dataset based on start time and warehuuse name.
        dataset = dataset\
            .filter(F.col('QUERY_PARAMETERIZED_HASH') == self.qp_hash)

        dataset = dataset.with_column('PERIOD',
                                      F.when(F.col('START_TIME') < self.date_split, F.lit('After'))
                                      .otherwise('Before'))

        return dataset

    def set_qp_hahs(self, qp_hash: str = None) -> None:
        if isinstance(qp_hash, str):
            self.qp_hash = qp_hash
        else:
            raise TypeError(f"{qp_hash} is not a string.")

    def _fit(self, dataset: snowpark.DataFrame) -> "execution_time_history":
        return self

    def fit(self, dataset: snowpark.DataFrame) -> "execution_time_history":
        return self
