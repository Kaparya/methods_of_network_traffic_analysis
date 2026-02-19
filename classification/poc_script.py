from __future__ import annotations

import logging
import sys
from pathlib import Path

import matplotlib
import numpy as np
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from src.core import PipelineContext
from src.pipeline import build_pipeline
from src.utils import resolve_csv, plot_class_balance, print_and_save_report

# Use Agg backend for headless environments
matplotlib.use('Agg')

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

OUTPUT_DIR = Path(".")


def main() -> None:
    """Main function to run the classification PoC."""
    # 1. Run the data pipeline.
    csv_path = resolve_csv()
    pipeline = build_pipeline()

    logging.info(f"Starting pipeline (source: {csv_path}) ...")
    ctx = PipelineContext(csv_path=csv_path)
    try:
        ctx = pipeline.handle(ctx)
    except Exception:
        logging.exception("Pipeline failed")
        sys.exit(1)

    features = ctx.features
    target = ctx.target
    feature_names: list[str] = getattr(
        ctx, 'feature_names', [f"feat_{i}" for i in range(features.shape[1])]
    )
    logging.info(f"Data ready.  features shape: {features.shape},  target classes: {np.unique(target)}")

    # 2. Class balance.
    unique, counts = np.unique(target, return_counts=True)
    logging.info("Class Balance:")
    for label, count in zip(unique, counts):
        logging.info(f"  {label}: {count:>6}  ({count / len(target) * 100:.1f}%)")

    plot_class_balance(target, OUTPUT_DIR / "grade_distribution.png")

    # 3. Encode target labels.
    le = LabelEncoder()
    y_enc = le.fit_transform(target)

    # 4. Train / test split — use the FULL dataset.
    X_train, X_test, y_train, y_test = train_test_split(
        features, y_enc, test_size=0.2, random_state=42, stratify=y_enc,
    )
    logging.info(f"Train size: {X_train.shape[0]},  Test size: {X_test.shape[0]}")

    # 5. Train CatBoost.
    logging.info("Training CatBoost Classifier ...")
    clf = CatBoostClassifier(
        iterations=300,
        learning_rate=0.1,
        depth=6,
        loss_function='MultiClass',
        auto_class_weights='Balanced',
        random_seed=42,
        verbose=50,
        allow_writing_files=False,
        thread_count=-1,
    )
    clf.fit(X_train, y_train)

    # 6. Evaluate.
    y_pred = clf.predict(X_test).flatten()
    print_and_save_report(
        y_test, y_pred, le.classes_,
        feature_names, clf.feature_importances_,
        OUTPUT_DIR / "classification_report.txt",
    )


if __name__ == "__main__":
    main()
