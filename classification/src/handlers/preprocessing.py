import logging
import pandas as pd
from src.core import Handler, PipelineContext

class EncodeCategoricalFeaturesHandler(Handler):
    """
    Handler for encoding categorical features.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"EncodeCategoricalFeaturesHandler: Start with {ctx.df.shape[1]} features")
        df = ctx.df.copy()

        # We must NOT encode the target 'grade' if it's already created.
        # But 'grade' is string (object), so get_dummies will convert it.
        # We should exclude 'grade' from dummy encoding.
        
        target_col = 'grade'
        cat_cols = df.select_dtypes(include="object").columns.tolist()
        
        if target_col in cat_cols:
            cat_cols.remove(target_col)
            
        df = pd.get_dummies(df, columns=cat_cols, drop_first=True)

        ctx.df = df
        logging.info(f"EncodeCategoricalFeaturesHandler: Updated df with {ctx.df.shape[1]} features")
        return ctx

class SplitDataHandler(Handler):
    """
    Handler for splitting the dataset into features and target.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"SplitDataHandler: Splitting data into features and target")
        df = ctx.df.copy()

        ctx.X = df.drop(columns=["salary_rub"])
        ctx.y = df["salary_rub"]

        ctx.df = None
        logging.info(f"SplitDataHandler: df was split into X and y.")
        return ctx

class SplitClassificationDataHandler(Handler):
    """
    Handler for splitting the dataset into features and target (grade) for classification.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"SplitClassificationDataHandler: Splitting data into features and target")
        df = ctx.df.copy()

        # Drop salary if it exists, as it might be a target leakage or not available at inference?
        # The prompt says "predict level... by salary, city, age...". So Salary IS a feature.
        # So we keep salary_rub.
        
        target_col = 'grade'
        if target_col not in df.columns:
            logging.error("Grade column not found!")
            return ctx
            
        ctx.y = df[target_col].values
        ctx.X = df.drop(columns=[target_col]).values # Convert to numpy array to match interface
        
        # We might want to keep feature names
        ctx.feature_names = df.drop(columns=[target_col]).columns.tolist()

        ctx.df = None # Clear df to save memory
        logging.info(f"SplitClassificationDataHandler: df was split into X and y.")
        return ctx
