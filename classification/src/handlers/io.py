import logging
import pandas as pd
import numpy as np

from src.core import Handler, PipelineContext

class LoadCSVHandler(Handler):
    """
    Handler for loading data from a CSV file.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"LoadCSVHandler: Starting to load {ctx.csv_path}")
        ctx.df = pd.read_csv(
            ctx.csv_path,
            sep=",",
            quotechar='"',
            engine="python",
            encoding="utf-8",
            index_col=0
        )
        logging.info(f"LoadCSVHandler: Loaded {ctx.csv_path} with {ctx.df.shape[0]} rows and {ctx.df.shape[1]} columns")
        return ctx

class SaveDataHandler(Handler):
    """
    Handler for saving the dataset into X.npy and y.npy files.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"SaveDataHandler: Saving data")
        np.save("X.npy", ctx.X)
        np.save("y.npy", ctx.y)
        logging.info(f"SaveDataHandler: Data was saved to X.npy and y.npy files")
        return ctx
