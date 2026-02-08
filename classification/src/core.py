from __future__ import annotations

import sys
import logging
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
    """
    Context for processing data pipeline.

    Attributes:
        csv_path: Path to the original CSV file.
        df: dataframe with data (default None).
        X: features (default None).
        y: target (default None).
    """
    csv_path: Path
    df: Optional[pd.DataFrame] = None
    X: Optional[np.ndarray] = None
    y: Optional[np.ndarray] = None

class Handler(ABC):
    """
    Abstract handler for implementing a chain of responsibility.

    Methods:
        set_next(handler): Sets the next handler in the chain.
        handle(ctx): Processes the data context and passes it to the next handler in the chain.
        _process(ctx): Abstract method for specific processing, must be implemented in subclasses.
    """
    def __init__(self):
        self._next: Optional["Handler"] = None

    def set_next(self, handler: "Handler") -> "Handler":
        self._next = handler
        return handler

    def handle(self, ctx: PipelineContext) -> PipelineContext:
        ctx = self._process(ctx)
        if self._next:
            return self._next.handle(ctx)
        return ctx

    @abstractmethod
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        ...
