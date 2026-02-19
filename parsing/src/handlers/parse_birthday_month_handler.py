from src.core import Handler, PipelineContext

import logging

class ParseBirthdayMonthHandler(Handler):
    """
    Handler for parsing birthday month information from the "Пол, возраст" column.

    Methods:
        _process(ctx): Extracts birthday month information from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info("ParseBirthdayMonthHandler: Starting to parse birthday month")
        dataframe = ctx.dataframe.copy()
        
        def extract_birthday_month(value: str) -> int:
            data = value.split(',')
            if len(data) < 3:
                return -1
            raw_birthday_month = data[2].strip().replace('\xa0', ' ')
            raw_birthday_month = raw_birthday_month.split(' ')[-2]
            match raw_birthday_month:
                case 'January' | 'января': return 0
                case 'February' | 'февраля': return 1
                case 'March' | 'марта': return 2
                case 'April' | 'апреля': return 3
                case 'May' | 'мая': return 4
                case 'June' | 'июня': return 5
                case 'July' | 'июля': return 6
                case 'August' | 'августа': return 7
                case 'September' | 'сентября': return 8
                case 'October' | 'октября': return 9
                case 'November' | 'ноября': return 10
                case 'December' | 'декабря': return 11
                case _: return -1

        dataframe["birthday_month"] = dataframe["Пол, возраст"].apply(extract_birthday_month)

        if "Пол, возраст" in dataframe.columns:
            dataframe = dataframe.drop(columns=["Пол, возраст"])

        ctx.dataframe = dataframe
        logging.info("ParseBirthdayMonthHandler: Parsed birthday month and dropped source column")
        return ctx
