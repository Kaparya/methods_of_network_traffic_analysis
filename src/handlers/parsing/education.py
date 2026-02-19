import logging
from src.core import Handler, PipelineContext

class ParseEducationHandler(Handler):
    """Handler for parsing education information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Parse 'Образование и ВУЗ' into binary education level flags.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with education flags.
        """
        logging.info("ParseEducationHandler: Parsing education")
        dataframe = ctx.dataframe.copy()
        
        mapping = {
            "incomplete_higher": ["неоконченное высшее", "incomplete higher"],
            "higher": ["высшее образование", "higher education"],
            "secondary_special": ["среднее специальное", "secondary special"],
            "secondary": ["среднее образование", "secondary education"]
        }

        for column_suffix, keywords in mapping.items():
            dataframe[f"edu_{column_suffix}"] = dataframe["Образование и ВУЗ"].apply(
                lambda s, kw=keywords: 1 if any(k in s.lower() for k in kw) else 0
            )

        ctx.dataframe = dataframe.drop(columns=["Образование и ВУЗ"])
        logging.info("ParseEducationHandler: Done")
        return ctx
