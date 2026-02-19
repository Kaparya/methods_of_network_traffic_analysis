from src.core import Handler, PipelineContext

import logging

class ParseAgeHandler(Handler):
    """
    Handler for parsing age information from the "Пол, возраст" column.

    Methods:
        _process(ctx): Extracts age information from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info("ParseAgeHandler: Starting to parse age")
        dataframe = ctx.dataframe.copy()
        
        def extract_age(value: str) -> int:
            data = value.split(',')
            if len(data) < 2:
                return -1
            raw_age = data[1].strip().replace('\xa0', ' ')
            raw_age = raw_age.split(' ')[0]
            try:
                return int(raw_age)
            except (ValueError, IndexError):
                return -1

        dataframe["age"] = dataframe["Пол, возраст"].apply(extract_age)
        ctx.dataframe = dataframe
        logging.info("ParseAgeHandler: Parsed age")
        return ctx
