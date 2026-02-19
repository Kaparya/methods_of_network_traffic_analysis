import logging
import re
from src.core import Handler, PipelineContext

class ParseExperienceHandler(Handler):
    """Handler for parsing experience information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'Опыт' string into total months.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with 'experience_months' column.
        """
        logging.info("ParseExperienceHandler: Parsing experience")
        dataframe = ctx.dataframe.copy()

        def extract(value: str) -> int:
            if not isinstance(value, str): 
                return 0
            years_match = re.search(r'(\d+)\s*(?:год|года|лет)', value)
            months_match = re.search(r'(\d+)\s*(?:месяц|месяца|месяцев)', value)
            total = 0
            if years_match:
                total += int(years_match.group(1)) * 12
            if months_match:
                total += int(months_match.group(1))
            return total
        
        dataframe["experience_months"] = dataframe["Опыт (двойное нажатие для полной версии)"].apply(extract)
        ctx.dataframe = dataframe
        logging.info("ParseExperienceHandler: Done")
        return ctx
