from src.core import Handler, PipelineContext

import logging

class ParseLastJobHandler(Handler):
    """
    Handler for parsing last job information from the "Последеняя/нынешняя должность" column.

    Methods:
        _process(ctx): Extracts new columns for last job from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseLastJobHandler: Starting to parse last job")
        dataframe = ctx.dataframe.copy()

        # lets take jobs from jobs column and parse only them
        jobs = dataframe['job'].value_counts()

        def extract_job(value: str) -> str:
            if value in jobs:
                return value
            return "other"

        dataframe["last_job"] = dataframe["Последеняя/нынешняя должность"].apply(extract_job)

        dataframe = dataframe.drop(columns=["Последеняя/нынешняя должность"])

        ctx.dataframe = dataframe
        logging.info(f"ParseLastJobHandler: Parsed last job")
        return ctx
