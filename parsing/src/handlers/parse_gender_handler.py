from src.core import Handler, PipelineContext

import logging

class ParseGenderHandler(Handler):
    """
    Handler for parsing gender information from the "Пол, возраст" column.

    Methods:
        _process(ctx): Extracts gender information from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info("ParseGenderHandler: Starting to parse gender")
        dataframe = ctx.dataframe.copy()
        
        # document possible gender values
        male_values = ['Мужчина', 'Male']
        # female_values = ['Женщина', 'Female']
        # also encode them with 0 - male, 1 - female
        def extract_gender(value: str) -> int:
            raw_gender = value.split(',')[0].strip()
            if raw_gender in male_values:
                return 0 # Male
            return 1 # Female

        dataframe["gender"] = dataframe["Пол, возраст"].apply(extract_gender)
        ctx.dataframe = dataframe
        logging.info("ParseGenderHandler: Parsed gender")
        return ctx
