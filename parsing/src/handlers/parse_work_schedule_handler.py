from src.core import Handler, PipelineContext

import logging

class ParseWorkScheduleHandler(Handler):
    """
    Handler for parsing work schedule information from the "График" column.

    Methods:
        _process(ctx): Extracts new columns for work schedule from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseWorkScheduleHandler: Starting to parse work schedule")
        dataframe = ctx.dataframe.copy()

        schedule_map = {
            "full_day": ["полный день", "full day"],
            "flexible": ["гибкий график", "flexible schedule"],
            "shift": ["сменный график", "shift schedule"],
            "remote": ["удаленная работа", "remote working"],
            "rotation": ["вахтовый метод", "rotation based work"]
        }

        for column_name, keywords in schedule_map.items():
            def check_schedule(value: str) -> int:
                value_lower = value.lower()
                if any(keyword in value_lower for keyword in keywords):
                    return 1
                return 0
            
            dataframe[f"sch_{column_name}"] = dataframe["График"].apply(check_schedule)

        dataframe = dataframe.drop(columns=["График"])

        ctx.dataframe = dataframe
        logging.info(f"ParseWorkScheduleHandler: Parsed work schedule")
        return ctx
