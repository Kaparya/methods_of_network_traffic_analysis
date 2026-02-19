from src.core import Handler, PipelineContext

import logging

class ParseEmploymentHandler(Handler):
    """
    Handler for parsing employment information from the "Занятость" column.

    Methods:
        _process(ctx): Extracts new columns for employment from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseEmploymentHandler: Starting to parse employment")
        dataframe = ctx.dataframe.copy()

        # group employment
        employment_map = {
            "full_time": ["полная занятость", "full time"],
            "part_time": ["частичная занятость", "part time"],
            "project": ["проектная работа", "project work"],
            "internship": ["стажировка", "work placement"],
            "volunteering": ["волонтерство", "volunteering"]
        }

        for column_name, keywords in employment_map.items():
            def check_employment(value: str) -> int:
                value_lower = value.lower()
                if any(keyword in value_lower for keyword in keywords):
                    return 1
                return 0
            
            dataframe[f"emp_{column_name}"] = dataframe["Занятость"].apply(check_employment)

        dataframe = dataframe.drop(columns=["Занятость"])

        ctx.dataframe = dataframe
        logging.info(f"ParseEmploymentHandler: Parsed employment")
        return ctx
