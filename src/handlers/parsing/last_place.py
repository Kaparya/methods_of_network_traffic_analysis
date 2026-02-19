import logging
from src.core import Handler, PipelineContext

class ParseLastPlaceHandler(Handler):
    """Handler for parsing last place information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Drop 'Последенее/нынешнее место работы' column.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context with the column removed.
        """
        logging.info("ParseLastPlaceHandler: Dropping column")
        ctx.dataframe = ctx.dataframe.drop(columns=["Последенее/нынешнее место работы"])
        return ctx
