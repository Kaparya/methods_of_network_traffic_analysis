from config import logger
from data_loader import load_data
from inference import predict_and_save
from model import evaluate_model, save_model, train_model

import argparse
from pathlib import Path
import sys

def parse_arguments():
    """Parse CLI arguments for training or inference modes."""
    parser = argparse.ArgumentParser(description="Salary Prediction Model CLI")

    group = parser.add_mutually_exclusive_group(required=True)
    
    group.add_argument(
        "--train", 
        action="store_true", 
        help="Train the model using data from parsing directory"
    )
    
    group.add_argument(
        "input_file", 
        nargs="?", 
        help="Path to input .npy file for prediction"
    )

    return parser.parse_args()

def main():
    """Main entry point handling argument parsing and pipeline execution."""
    args = parse_arguments()

    try:
        if args.train:
            logger.info("Starting training process...")
            X, y = load_data()
            
            model = train_model(X, y)
            save_model(model)

            logger.info("Training completed successfully.")            
        elif args.input_file:
            input_path = Path(args.input_file)
            logger.info(f"Starting inference for file: {input_path}")
            
            output_path = input_path.parent / "y_pred.npy"
            
            y_pred = predict_and_save(str(input_path), str(output_path))
            try:
                _, y_true = load_data()
                evaluate_model(y_true, y_pred)
            except Exception as e: 
                logger.info(f'Metrics evaluation is not possible, unknown dataset: {e}')
            
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
