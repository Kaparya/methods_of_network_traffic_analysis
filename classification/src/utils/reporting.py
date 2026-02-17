import logging
from pathlib import Path

import numpy as np
from sklearn.metrics import classification_report

def print_and_save_report(
    y_test,
    y_pred,
    class_names,
    feature_names,
    importances,
    path: Path,
) -> None:
    """
    Print classification report and top features; save to file.

    Args:
        y_test: True labels.
        y_pred: Predicted labels.
        class_names: List of class names.
        feature_names: List of feature names.
        importances: Array of feature importance scores.
        path: Output path for the report text file.
    """
    report = classification_report(y_test, y_pred, target_names=class_names)
    logging.info("=" * 60)
    logging.info("Classification Report")
    logging.info("=" * 60)
    logging.info(f"\n{report}")

    top_n = min(15, len(feature_names))
    order = np.argsort(importances)[::-1]

    logging.info(f"Top {top_n} Important Features:")
    for i in range(top_n):
        idx = order[i]
        logging.info(f"  {i + 1:>2}. {feature_names[idx]:<40s} {importances[idx]:.4f}")

    with open(path, "w", encoding="utf-8") as fout:
        fout.write("Classification Report\n")
        fout.write("=" * 60 + "\n\n")
        fout.write(report)
        fout.write(f"\n\nTop {top_n} Features:\n")
        for i in range(top_n):
            idx = order[i]
            fout.write(f"{feature_names[idx]}: {importances[idx]:.4f}\n")
    logging.info(f"Saved report -> {path}")
