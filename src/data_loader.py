from src.config import X_PATH, Y_PATH, logger

import numpy as np

def load_data():
    """
    Load preprocessed dataset (features and target) from the parsing directory.
    
    Returns:
        tuple containing the feature matrix (features) and target vector (target).
    """
    if not X_PATH.exists() or not Y_PATH.exists():
        raise FileNotFoundError(f"Files not found: {X_PATH} or {Y_PATH}. Please run parsing pipeline first.")
        
    logger.info("Loading data...")
    features = np.load(X_PATH, allow_pickle=True)
    target = np.load(Y_PATH, allow_pickle=True)
    logger.info(f"Data loaded. features shape: {features.shape}, target shape: {target.shape}")
    return features, target
