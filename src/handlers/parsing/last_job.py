import logging
from src.core import Handler, PipelineContext

class ParseLastJobHandler(Handler):
    """Handler for parsing last job information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Normalize 'Последеняя/нынешняя должность' to top 133 or 'other'.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with 'last_job' column.
        """
        logging.info("ParseLastJobHandler: Parsing last job")
        dataframe = ctx.dataframe.copy()
        
        # Note: ideally reuse top_jobs from ParseJobHandler
        jobs = dataframe['job'].value_counts().index if 'job' in dataframe else []
        
        dataframe["last_job"] = dataframe["Последеняя/нынешняя должность"].apply(
            lambda job_title: job_title if job_title in jobs else "other"
        )
        ctx.dataframe = dataframe.drop(columns=["Последеняя/нынешняя должность"])
        logging.info("ParseLastJobHandler: Done")
        return ctx
