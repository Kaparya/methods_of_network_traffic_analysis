from src.core import Handler, PipelineContext

import logging

class ParseResumeHandler(Handler):
    """
    Handler for parsing resume information from the "Обновление резюме" column. 
    Splits resume in "old" (more than 1 year) and "not old" (less than 1 year).

    Methods:
        _process(ctx): Extracts new columns for resume from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseResumeHandler: Starting to parse resume")
        dataframe = ctx.dataframe.copy()

        def extract_oldness(value: str) -> str:
            try:
                year = int(value.split('.')[2].split(' ')[0])
            except Exception as e:
                logging.error(f"ParseResumeHandler: Error extracting oldness: {e}")
                year = 0
            return 0 if year > 2018 else 1

        dataframe["old_resume"] = dataframe["Обновление резюме"].apply(extract_oldness)

        dataframe = dataframe.drop(columns=["Обновление резюме"])

        ctx.dataframe = dataframe
        logging.info(f"ParseResumeHandler: Parsed resume")
        return ctx
