from src.core import Handler, PipelineContext

import logging
import re

class ParseExperienceHandler(Handler):
    """
    Handler for parsing experience information from the "Опыт (двойное нажатие для полной версии)" column.

    Methods:
        _process(ctx): Extracts new columns for experience from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseExperienceHandler: Starting to parse experience")
        dataframe = ctx.dataframe.copy()

        def check_experience(value: str) -> int:
            years_pattern = r'(\d+)\s*(?:год|года|лет)'
            months_pattern = r'(\d+)\s*(?:месяц|месяца|месяцев)'
            
            experience_part = value.split('\n')[0]
            
            years = re.search(years_pattern, experience_part)
            months = re.search(months_pattern, experience_part)
            total_months = 0
            if years:
                total_months += int(years.group(1)) * 12
            if months:
                total_months += int(months.group(1))
                
            return total_months
        
        dataframe["experience_months"] = dataframe["Опыт (двойное нажатие для полной версии)"].apply(check_experience)

        dataframe = dataframe.drop(columns=["Опыт (двойное нажатие для полной версии)"])

        ctx.dataframe = dataframe
        logging.info(f"ParseExperienceHandler: Parsed experience")
        return ctx
