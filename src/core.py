from __future__ import annotations

import logging
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    stream=sys.stdout
)

@dataclass
class PipelineContext:
    """Context for processing data pipeline."""
    csv_path: Path
    dataframe: Optional[pd.DataFrame] = None
    features: Optional[np.ndarray] = None
    target: Optional[np.ndarray] = None

class Handler(ABC):
    """Abstract handler for chain of responsibility pattern."""
    def __init__(self):
        self._next: Optional["Handler"] = None

    def set_next(self, handler: "Handler") -> "Handler":
        """
        Set the next handler in the chain.

        Args:
            handler: The next handler to be executed.

        Returns:
            Handler: The handler that was just set (for chaining).
        """
        self._next = handler
        return handler

    def handle(self, ctx: PipelineContext) -> PipelineContext:
        """
        Handle the request and pass it to the next handler.

        Args:
            ctx: The data pipeline context.

        Returns:
            PipelineContext: The processed context.
        """
        ctx = self._process(ctx)
        if self._next:
            return self._next.handle(ctx)
        return ctx

    @abstractmethod
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Process the context (abstract method).

        Args:
            ctx: The data pipeline context.

        Returns:
            PipelineContext: The processed context.
        """
        ...
