from config import RESOURCES_DIR, MODEL_PATH, logger

from catboost import CatBoostRegressor
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

def evaluate_model(y_true, y_pred):
    """
    Calculate and log regression performance metrics (MAE, RMSE, R2).

    Args:
        y_true: Actual target values.
        y_pred: Predicted target values.

    Returns:
        Dictionary containing calculated metrics.
    """
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    
    logger.info("Model Evaluation:")
    logger.info(f"MAE: {mae:.2f}")
    logger.info(f"RMSE: {np.sqrt(mse):.2f}")
    logger.info(f"R2 Score: {r2:.4f}")
    
    return {"mae": mae, "rmse": np.sqrt(mse), "r2": r2}

def train_model(X, y):
    """
    Train a CatBoostRegressor model on the provided data.
    
    Splits data 80/20, trains the model, and evaluates on test set.

    Args:
        X: Feature matrix.
        y: Target vector.

    Returns:
        Trained CatBoostRegressor model.
    """
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=28)
    
    model = CatBoostRegressor(verbose=0, random_state=28)
    
    logger.info("Training CatBoost model...")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    evaluate_model(y_test, y_pred)
    return model

def save_model(model):
    """
    Save the trained model to the resources directory in .cbm format.

    Args:
        model: Trained CatBoostRegressor model.
    """
    RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
    model.save_model(str(MODEL_PATH))
    logger.info(f"Model saved to {MODEL_PATH}")
