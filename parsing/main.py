from src.pipeline import build_pipeline
from src.core import PipelineContext

import logging
from pathlib import Path

def main():
    """
    Main function to run the pipeline.
    """
    logging.info("Starting the pipeline")   
    pipeline = build_pipeline()
    ctx = PipelineContext(csv_path=Path("hh.csv"))
    ctx = pipeline.handle(ctx)
    logging.info("Pipeline completed")

if __name__ == "__main__":
    main()
