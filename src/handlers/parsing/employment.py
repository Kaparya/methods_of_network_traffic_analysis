import logging
from src.core import Handler, PipelineContext

class ParseEmploymentHandler(Handler):
    """Handler for parsing employment information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'Занятость' column into binary flags.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with employment flags.
        """
        logging.info("ParseEmploymentHandler: Starting to parse employment")
        dataframe = ctx.dataframe.copy()

        employment_map = {
            "full_time": ["полная занятость", "full time"],
            "part_time": ["частичная занятость", "part time"],
            "project": ["проектная работа", "project work"],
            "internship": ["стажировка", "work placement"],
            "volunteering": ["волонтерство", "volunteering"]
        }

        for column_name, keywords in employment_map.items():
            def check_employment(value: str, kw=keywords) -> int:
                value_lower = value.lower()
                if any(keyword in value_lower for keyword in kw):
                    return 1
                return 0
            
            dataframe[f"emp_{column_name}"] = dataframe["Занятость"].apply(check_employment)

        dataframe = dataframe.drop(columns=["Занятость"])

        ctx.dataframe = dataframe
        logging.info("ParseEmploymentHandler: Parsed employment")
        return ctx
