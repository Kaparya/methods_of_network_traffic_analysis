import logging
from src.core import Handler, PipelineContext

class ParseWorkScheduleHandler(Handler):
    """Handler for parsing work schedule information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'График' column into binary flags.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with schedule flags.
        """
        logging.info("ParseWorkScheduleHandler: Starting to parse work schedule")
        dataframe = ctx.dataframe.copy()

        schedule_map = {
            "full_day": ["полный день", "full day"],
            "flexible": ["гибкий график", "flexible schedule"],
            "shift": ["сменный график", "shift schedule"],
            "remote": ["удаленная работа", "remote working"],
            "rotation": ["вахтовый метод", "rotation based work"]
        }

        for column_name, keywords in schedule_map.items():
            def check_schedule(value: str, kw=keywords) -> int:
                value_lower = value.lower()
                if any(keyword in value_lower for keyword in kw):
                    return 1
                return 0
            
            dataframe[f"sch_{column_name}"] = dataframe["График"].apply(check_schedule)

        dataframe = dataframe.drop(columns=["График"])

        ctx.dataframe = dataframe
        logging.info("ParseWorkScheduleHandler: Parsed work schedule")
        return ctx
