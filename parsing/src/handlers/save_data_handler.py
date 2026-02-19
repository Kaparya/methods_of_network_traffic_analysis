from src.core import Handler, PipelineContext

import logging

import numpy as np

class SaveDataHandler(Handler):
    """
    Handler for saving the dataset into features.npy and target.npy files.

    Methods:
        _process(ctx): Saves the dataset into features.npy and target.npy files.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"SaveDataHandler: Saving data")
        np.save("features.npy", ctx.features)
        np.save("target.npy", ctx.target)
        logging.info(f"SaveDataHandler: Data was saved to features.npy and target.npy files")
        return ctx
