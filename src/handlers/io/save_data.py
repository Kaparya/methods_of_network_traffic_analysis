import logging
import numpy as np
from src.core import Handler, PipelineContext

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
