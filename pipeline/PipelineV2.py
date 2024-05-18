from snowflake.ml.modeling.pipeline import Pipeline
from snowflake import snowpark

from typing import Optional, Union

import pandas as pd


class PipelineV2(Pipeline):
    def fit(self,
            dataset: Union[snowpark.DataFrame, pd.DataFrame],
            squash: Optional[bool] = False) -> "PipelineV2":
        self._is_fitted = True
        return self
