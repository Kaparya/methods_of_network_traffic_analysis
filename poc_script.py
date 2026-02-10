from __future__ import annotations

import logging
import sys
import traceback
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from catboost import CatBoostClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from src.core import PipelineContext
from src.pipeline import build_pipeline

# Use Agg backend for headless environments
matplotlib.use('Agg')

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

OUTPUT_DIR = Path(".")


def _resolve_csv() -> Path:
    """
    Find hh.csv in several common locations.

    Returns:
        Path: Path to the existing hh.csv file.

    Raises:
        SystemExit: If file is not found.
    """
    candidates = [
        Path("parsing/hh.csv"),
        Path("../parsing/hh.csv"),
        Path("hh.csv"),
    ]
    for candidate_path in candidates:
        if candidate_path.exists():
            return candidate_path
    print("Error: hh.csv not found.")
    sys.exit(1)


def _plot_class_balance(labels: np.ndarray, path: Path) -> None:
    """
    Save a bar-chart of class distribution.

    Args:
        labels: Array of target labels.
        path: Output path for the image file.
    """
    unique, counts = np.unique(labels, return_counts=True)

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(unique, counts, color=['#4CAF50', '#2196F3', '#FF9800'])
    ax.set_title("Distribution of Developer Grades (IT resumes from hh.ru)")
    ax.set_xlabel("Grade")
    ax.set_ylabel("Number of resumes")

    for bar, count in zip(bars, counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2, bar.get_height() + 50,
            str(count), ha='center', va='bottom', fontweight='bold',
        )

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"Saved class balance plot -> {path}")


def _print_and_save_report(
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
    print("\n" + "=" * 60)
    print("Classification Report")
    print("=" * 60)
    print(report)

    top_n = min(15, len(feature_names))
    order = np.argsort(importances)[::-1]

    print(f"Top {top_n} Important Features:")
    for i in range(top_n):
        idx = order[i]
        print(f"  {i + 1:>2}. {feature_names[idx]:<40s} {importances[idx]:.4f}")

    with open(path, "w", encoding="utf-8") as fout:
        fout.write("Classification Report\n")
        fout.write("=" * 60 + "\n\n")
        fout.write(report)
        fout.write(f"\n\nTop {top_n} Features:\n")
        for i in range(top_n):
            idx = order[i]
            fout.write(f"{feature_names[idx]}: {importances[idx]:.4f}\n")
    print(f"\nSaved report -> {path}")


def main() -> None:
    """Main function to run the classification PoC."""
    # 1. Run the data pipeline.
    csv_path = _resolve_csv()
    pipeline = build_pipeline()

    print(f"Starting pipeline (source: {csv_path}) ...")
    ctx = PipelineContext(csv_path=csv_path)
    try:
        ctx = pipeline.handle(ctx)
    except Exception as exc:
        logging.error(f"Pipeline failed: {exc}")
        traceback.print_exc()
        sys.exit(1)

    features = ctx.features
    target = ctx.target
    feature_names: list[str] = getattr(
        ctx, 'feature_names', [f"feat_{i}" for i in range(features.shape[1])]
    )
    print(f"\nData ready.  X shape: {features.shape},  y classes: {np.unique(target)}")

    # 2. Class balance.
    unique, counts = np.unique(target, return_counts=True)
    print("\nClass Balance:")
    for label, count in zip(unique, counts):
        print(f"  {label}: {count:>6}  ({count / len(target) * 100:.1f}%)")

    _plot_class_balance(target, OUTPUT_DIR / "grade_distribution.png")

    # 3. Encode target labels.
    le = LabelEncoder()
    y_enc = le.fit_transform(target)

    # 4. Train / test split — use the FULL dataset.
    X_train, X_test, y_train, y_test = train_test_split(
        features, y_enc, test_size=0.2, random_state=42, stratify=y_enc,
    )
    print(f"\nTrain size: {X_train.shape[0]},  Test size: {X_test.shape[0]}")

    # 5. Train CatBoost.
    print("\nTraining CatBoost Classifier ...")
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
    _print_and_save_report(
        y_test, y_pred, le.classes_,
        feature_names, clf.feature_importances_,
        OUTPUT_DIR / "classification_report.txt",
    )


if __name__ == "__main__":
    main()
