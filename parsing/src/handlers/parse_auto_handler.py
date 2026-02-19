from src.core import Handler, PipelineContext

import logging

class ParseAutoHandler(Handler):
    """
    Handler for parsing auto information from the "Авто" column. 

    Methods:
        _process(ctx): Extracts new columns for auto from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseAutoHandler: Starting to parse auto")
        dataframe = ctx.dataframe.copy()

        def extract_auto(value: str) -> str:
            match value:
                case 'Имеется собственный автомобиль':
                    return 1
                case 'Не указано':
                    return 0
                case _:
                    return 0

        dataframe["auto"] = dataframe["Авто"].apply(extract_auto)

        dataframe = dataframe.drop(columns=["Авто"])

        ctx.dataframe = dataframe
        logging.info(f"ParseAutoHandler: Parsed auto")
        return ctx
