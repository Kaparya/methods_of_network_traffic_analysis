import logging
from src.core import Handler, PipelineContext

class SplitDataHandler(Handler):
    """Handler for splitting data for regression (salary target)."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Split dataframe into features and target (salary_rub).

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context with features and target populated.
        """
        logging.info("SplitDataHandler: Splitting features/target")
        ctx.features = ctx.dataframe.drop(columns=["salary_rub"])
        ctx.target = ctx.dataframe["salary_rub"]
        ctx.dataframe = None
        return ctx
