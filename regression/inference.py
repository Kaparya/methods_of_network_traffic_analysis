from config import MODEL_PATH, logger

from pathlib import Path

from catboost import CatBoostRegressor
import numpy as np

def predict_and_save(x_path_str: str, output_path_str: str = "y.npy") -> np.ndarray:
    """
    Load model and perform inference on input data.

    Args:
        x_path_str: Path to input .npy file with features.
        output_path_str: Path to save predictions (defaults to "y.npy").

    Returns:
        Array of predicted values.
    """
    if not MODEL_PATH.exists():
        error_msg = f"Model file not found at {MODEL_PATH}. Please train the model first with `python3 main.py --train`"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
        
    logger.info(f"Loading model from {MODEL_PATH}")
    model = CatBoostRegressor()
    model.load_model(str(MODEL_PATH))
    
    x_path = Path(x_path_str)
    if not x_path.exists():
        raise FileNotFoundError(f"Input file not found: {x_path}")
        
    X_new = np.load(x_path, allow_pickle=True)
    
    logger.info(f"Predicting for {X_new.shape[0]} samples...")
    predictions = model.predict(X_new)
    
    np.save(output_path_str, predictions)
    logger.info(f"Predictions saved to {output_path_str}")
    
    return predictions
