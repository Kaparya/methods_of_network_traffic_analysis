from src.core import Handler, PipelineContext

import logging

import pandas as pd

class EncodeCategoricalFeaturesHandler(Handler):
    """
    Handler for encoding categorical features. 

    Methods:
        _process(ctx): Encodes categorical features.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"EncodeCategoricalFeaturesHandler: Start with {ctx.dataframe.shape[1]} features")
        dataframe = ctx.dataframe.copy()

        cat_cols = dataframe.select_dtypes(include="object").columns
        dataframe = pd.get_dummies(dataframe, columns=cat_cols, drop_first=True)

        ctx.dataframe = dataframe
        logging.info(f"EncodeCategoricalFeaturesHandler: Updated dataframe with {ctx.dataframe.shape[1]} features")
        return ctx
