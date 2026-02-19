from src.core import Handler, PipelineContext

import logging

class SplitDataHandler(Handler):
    """
    Handler for splitting the dataset into features and target.

    Methods:
        _process(ctx): Splits the dataset into features and target.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"SplitDataHandler: Splitting data into features and target")
        dataframe = ctx.dataframe.copy()

        ctx.features = dataframe.drop(columns=["salary_rub"])
        ctx.target = dataframe["salary_rub"]

        ctx.dataframe = None
        logging.info(f"SplitDataHandler: dataframe was split into features and target.")
        return ctx
