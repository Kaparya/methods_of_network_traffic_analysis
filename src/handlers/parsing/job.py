import logging
from src.core import Handler, PipelineContext

class ParseJobHandler(Handler):
    """Handler for parsing job information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Normalize job titles to the top 133 most common ones, grouping others as 'other'.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with normalized 'job' column.
        """
        logging.info("ParseJobHandler: Starting to parse job")
        dataframe = ctx.dataframe.copy()

        job_count = dataframe['Ищет работу на должность:'].value_counts()[:133]

        def extract_job(value: str) -> str:
            if value in job_count:
                return value
            return "other"

        dataframe["job"] = dataframe["Ищет работу на должность:"].apply(extract_job)
        dataframe = dataframe.drop(columns=["Ищет работу на должность:"])

        ctx.dataframe = dataframe
        logging.info("ParseJobHandler: Parsed job")
        return ctx
