import logging
from src.core import Handler, PipelineContext

class ParseGenderAgeBirthdayHandler(Handler):
    """Handler for parsing gender, age, and birthday information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'Пол, возраст' column into separate gender, age, and birthday_month columns.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with parsed features.
        """
        logging.info("ParseGenderAgeBirthdayHandler: Starting to parse gender, age and birthday month")
        dataframe = ctx.dataframe.copy()
        
        male_values = ['Мужчина', 'Male']
        
        def extract_gender(value: str) -> int:
            raw_gender = value.split(',')[0].strip()
            if raw_gender in male_values:
                return 0 # Male
            return 1 # Female

        def extract_age(value: str) -> int:
            data = value.split(',')
            if len(data) < 2:
                return -1
            raw_age = data[1].strip().replace('\xa0', ' ')
            raw_age = raw_age.split(' ')[0]
            return int(raw_age)

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

        GENDER_AGE_COL = "Пол, возраст"
        gender_data = dataframe[GENDER_AGE_COL].apply(extract_gender)
        age_data = dataframe[GENDER_AGE_COL].apply(extract_age)
        month_data = dataframe[GENDER_AGE_COL].apply(extract_birthday_month)

        dataframe["gender"] = gender_data
        dataframe["age"] = age_data
        dataframe["birthday_month"] = month_data

        dataframe = dataframe.drop(columns=[GENDER_AGE_COL])

        ctx.dataframe = dataframe
        logging.info("ParseGenderAgeHandler: Parsed gender, age and birthday month")
        return ctx
