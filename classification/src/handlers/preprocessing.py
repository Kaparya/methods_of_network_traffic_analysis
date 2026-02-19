import logging

import pandas as pd

from src.core import Handler, PipelineContext

class EncodeCategoricalFeaturesHandler(Handler):
    """Handler for encoding categorical features."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        One-hot encode categorical columns, excluding the target 'grade'.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context with encoded dataframe.
        """
        logging.info("EncodeCategoricalFeaturesHandler: Encoding categorical features")
        dataframe = ctx.dataframe.copy()

        # Exclude target 'grade' from encoding
        cat_cols = dataframe.select_dtypes(include="object").columns.tolist()
        if 'grade' in cat_cols:
            cat_cols.remove('grade')
            
        dataframe = pd.get_dummies(dataframe, columns=cat_cols, drop_first=True)
        ctx.dataframe = dataframe
        logging.info(f"EncodeCategoricalFeaturesHandler: Done (features: {dataframe.shape[1]})")
        return ctx

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
