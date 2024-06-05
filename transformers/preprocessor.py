import snowflake.snowpark.functions as F
from snowflake.ml.modeling.framework import base
from snowflake import snowpark

from typing import Iterable, List, Optional, Union
from datetime import datetime, date


class preprocessor(base.BaseTransformer):
    def __init__(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        wh_name: Optional[Union[str, Iterable[str]]] = None
    ) -> None:

        self.set_end_time(end_time)
        self.set_start_time(start_time)
        self.set_warehouse_name(wh_name)

        self.input_cols = []
        self.output_cols = []

    def transform(self, dataset: snowpark.DataFrame) -> snowpark.DataFrame:

        # Filters applied to the dataset based on start time and warehuuse name.
        dataset = dataset\
            .filter(F.col('START_TIME') <= self.end_time)\
            .filter(F.col('START_TIME') >= self.start_time)\
            .filter((F.lit(self.wh_name) == []) |
                    (F.col('WAREHOUSE_NAME').isin(self.wh_name) if self.wh_name else F.lit(True)))\
            .filter(F.col('WAREHOUSE_SIZE').is_not_null())

        # Selection of relevant columns and calculation of runtime in minutes.
        dataset = dataset.select(['WAREHOUSE_NAME',
                                  'QUERY_TAG',
                                  'QUERY_ID',
                                  'EXECUTION_TIME',
                                  'QUERY_PARAMETERIZED_HASH',
                                  'START_TIME'])

        return dataset

    def _process_warehouse_name(self, wh_name: Optional[Union[str, Iterable[str]]]) -> List[str]:
        """Convert wh_name to a list."""
        wh_name_list: List[str] = []
        if wh_name is None:
            return []
        elif type(wh_name) is list:
            wh_name_list = wh_name
        elif type(wh_name) is str:
            wh_name_list = [wh_name]
        else:
            wh_name_list = list(wh_name)

        return wh_name_list

    def val_warehouse_name(self, wh_name_list: Iterable[str] = None):
        for wh_name in wh_name_list:
            if " " in wh_name:
                raise ValueError(f"Not valid Warehuse name. The Warehuse name {wh_name} contains spaces.")

    def set_warehouse_name(self, wh_name: str = None):
        wh_name = self._process_warehouse_name(wh_name)
        self.val_warehouse_name(wh_name)
        self.wh_name = wh_name

    def val_dates(self):
        self.val_dates_order()
        self.val_within_last_14_days()

    def val_dates_order(self) -> None:
        if self.end_time < self.start_time:
            raise ValueError(f"{self.end_time} must be later than {self.start_time}")

    def val_within_last_14_days(self) -> None:
        current_date = datetime.now().date()

        end_days_diff = (current_date - self.end_time).days
        start_days_diff = (current_date - self.start_time).days

        if end_days_diff > 14:
            raise TypeError(f"{self.end_time} is more than 14 days in the past.")
        if start_days_diff > 14:
            raise TypeError(f"{self.start_time} is more than 14 days in the past.")

    def val_date(self, dates: date) -> None:
        if not isinstance(dates, date):
            raise TypeError(dates, "is not of type datetime")

    def set_start_time(self, start_time: datetime) -> None:
        """
        Start time setter.

        Args:
            start_time: A single datetime.

        Returns:
            self
        """
        self.val_date(start_time)
        self.start_time = start_time
        return self

    def set_end_time(self, end_time: datetime) -> None:
        """
        End time setter.

        Args:
            end_time: A single datetime.

        Returns:
            self
        """
        self.val_date(end_time)
        self.end_time = end_time
        return self

    def _fit(self, dataset: snowpark.DataFrame) -> "preprocessor":
        return self

    def fit(self, dataset: snowpark.DataFrame) -> "preprocessor":
        return self
