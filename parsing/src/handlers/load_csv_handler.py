from src.core import Handler, PipelineContext

import logging
import pandas as pd

class LoadCSVHandler(Handler):
    """
    Handler for loading data from a CSV file.

    Methods:
        _process(ctx): Loads data from a CSV file into the context.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"LoadCSVHandler: Starting to load {ctx.csv_path}")
        ctx.dataframe = pd.read_csv(
            ctx.csv_path,
            sep=",",
            quotechar='"',
            engine="python",
            encoding="utf-8",
            index_col=0
        )
        logging.info(f"LoadCSVHandler: Loaded {ctx.csv_path} with {ctx.dataframe.shape[0]} rows and {ctx.dataframe.shape[1]} columns")
        return ctx
