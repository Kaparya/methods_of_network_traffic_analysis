import logging
from src.core import Handler, PipelineContext

class ParseAutoHandler(Handler):
    """Handler for parsing auto information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'Авто' to create 'auto' ownership flag.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with 'auto' flag.
        """
        logging.info("ParseAutoHandler: Parsing auto")
        dataframe = ctx.dataframe.copy()
        dataframe["auto"] = dataframe["Авто"].apply(
            lambda auto_status: 1 if auto_status == 'Имеется собственный автомобиль' else 0
        )
        ctx.dataframe = dataframe.drop(columns=["Авто"])
        logging.info("ParseAutoHandler: Done")
        return ctx
