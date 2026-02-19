from src.core import Handler, PipelineContext

import logging

class ParseEducationHandler(Handler):
    """
    Handler for parsing education information from the "Образование и ВУЗ" column.

    Methods:
        _process(ctx): Extracts new columns for education from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseEducationHandler: Starting to parse education")
        dataframe = ctx.dataframe.copy()

        education_map = {
            "incomplete_higher": ["неоконченное высшее", "incomplete higher"],
            "higher": ["высшее образование", "higher education"],
            "secondary_special": ["среднее специальное", "secondary special"],
            "secondary": ["среднее образование", "secondary education"]
        }

        for column_name, keywords in education_map.items():
            def extract_level(value: str) -> int:
                value_lower = value.lower()
                if any(keyword in value_lower for keyword in keywords):
                    return 1
                return 0
            
            dataframe[f"edu_{column_name}"] = dataframe["Образование и ВУЗ"].apply(extract_level)

        dataframe = dataframe.drop(columns=["Образование и ВУЗ"])

        ctx.dataframe = dataframe
        logging.info(f"ParseEducationHandler: Parsed education")
        return ctx
