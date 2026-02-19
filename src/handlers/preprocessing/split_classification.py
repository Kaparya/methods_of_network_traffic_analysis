import logging
from src.core import Handler, PipelineContext

class SplitClassificationDataHandler(Handler):
    """Handler for splitting data for classification (grade target)."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Split dataframe into features and target (grade).

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context with features, target, and feature_names populated.
        """
        logging.info("SplitClassificationDataHandler: Splitting features/target")
        dataframe = ctx.dataframe.copy()
        
        if 'grade' not in dataframe.columns:
            logging.error("Grade column not found!")
            return ctx
            
        ctx.target = dataframe['grade'].values
        ctx.features = dataframe.drop(columns=['grade']).values
        ctx.feature_names = dataframe.drop(columns=['grade']).columns.tolist()
        ctx.dataframe = None
        logging.info("SplitClassificationDataHandler: Done")
        return ctx
