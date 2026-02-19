from src.core import Handler, PipelineContext

import logging

class ParseLastPlaceHandler(Handler):
    """
    Handler for parsing last place information from the "Последенее/нынешнее место работы" column.

    Methods:
        _process(ctx): Removes column for last place from the dataframe.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseLastPlaceHandler: Starting to parse last place")
        dataframe = ctx.dataframe.copy()

        # This column does not seem to be useful for the analysis, because we already use
        # last job column and city column. Therefore, we can drop this column.
        dataframe = dataframe.drop(columns=["Последенее/нынешнее место работы"])
        ctx.dataframe = dataframe
        logging.info(f"ParseLastPlaceHandler: Dropped last place column")
        return ctx
