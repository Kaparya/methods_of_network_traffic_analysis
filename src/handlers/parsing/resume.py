import logging
from src.core import Handler, PipelineContext

class ParseResumeHandler(Handler):
    """Handler for parsing resume information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'Обновление резюме' to create 'old_resume' flag.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with 'old_resume' flag.
        """
        logging.info("ParseResumeHandler: Parsing resume date")
        dataframe = ctx.dataframe.copy()

        def is_old(date_string: str) -> int:
            try:
                year = int(date_string.split('.')[2].split(' ')[0])
                return 0 if year > 2018 else 1
            except (ValueError, IndexError, AttributeError):
                return 0

        dataframe["old_resume"] = dataframe["Обновление резюме"].apply(is_old)
        ctx.dataframe = dataframe.drop(columns=["Обновление резюме"])
        logging.info("ParseResumeHandler: Done")
        return ctx
