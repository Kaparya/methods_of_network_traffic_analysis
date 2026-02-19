import logging
import pandas as pd
from src.core import Handler, PipelineContext

class EncodeCategoricalFeaturesHandler(Handler):
    """Handler for encoding categorical features."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        One-hot encode categorical columns, excluding the target 'grade'.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context with encoded dataframe.
        """
        logging.info("EncodeCategoricalFeaturesHandler: Encoding categorical features")
        dataframe = ctx.dataframe.copy()

        # Exclude target 'grade' from encoding
        cat_cols = dataframe.select_dtypes(include="object").columns.tolist()
        if 'grade' in cat_cols:
            cat_cols.remove('grade')
            
        dataframe = pd.get_dummies(dataframe, columns=cat_cols, drop_first=True)
        ctx.dataframe = dataframe
        logging.info(f"EncodeCategoricalFeaturesHandler: Done (features: {dataframe.shape[1]})")
        return ctx
