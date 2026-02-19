from src.core import Handler, PipelineContext

import logging

class ParseJobHandler(Handler):
    """
    Handler for parsing job information from the "Ищет работу на должность:" column.

    Methods:
        _process(ctx): Extracts new columns for job from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseJobHandler: Starting to parse job")
        dataframe = ctx.dataframe.copy()

        # due to pie chart of the distrubution of jobs, we can see 18007 different jobs.
        # However, there are only 133 jobs that are included more than 50 times.
        # So let's take only them - other jobs will be called "other" (as they make to much noise).

        # Also 133 jobs are much better for one-hot encoding (than 18007).
        job_count = dataframe['Ищет работу на должность:'].value_counts()[:133]

        def extract_job(value: str) -> str:
            if value in job_count:
                return value
            return "other"

        dataframe["job"] = dataframe["Ищет работу на должность:"].apply(extract_job)

        dataframe = dataframe.drop(columns=["Ищет работу на должность:"])

        ctx.dataframe = dataframe
        logging.info(f"ParseJobHandler: Parsed job")
        return ctx
