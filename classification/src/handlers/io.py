import logging

import numpy as np
import pandas as pd

from src.core import Handler, PipelineContext

class LoadCSVHandler(Handler):
    """Handler for loading data from a CSV file."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Load the CSV file specified in the context path into a DataFrame.

        Args:
            ctx: Pipeline context containing the path to the CSV file.

        Returns:
            PipelineContext: Context updated with the loaded DataFrame.
        """
        logging.info(f"LoadCSVHandler: Starting to load {ctx.csv_path}")
        ctx.dataframe = pd.read_csv(
            ctx.csv_path,
            sep=",",
            quotechar='"',
            engine="python",
            encoding="utf-8",
            index_col=0
        )
        logging.info(f"LoadCSVHandler: Loaded {ctx.csv_path} with {ctx.dataframe.shape[0]} rows and {ctx.dataframe.shape[1]} columns")
        return ctx

class SaveDataHandler(Handler):
    """Handler for saving the dataset into features.npy and target.npy files."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Save the processed features (features) and target (target) to NumPy files.

        Args:
            ctx: Pipeline context containing features and target arrays.

        Returns:
            PipelineContext: Unmodified context.
        """
        logging.info("SaveDataHandler: Saving data")
        np.save("features.npy", ctx.features)
        np.save("target.npy", ctx.target)
        logging.info("SaveDataHandler: Data was saved to features.npy and target.npy files")
        return ctx
