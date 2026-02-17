import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

def plot_class_balance(labels: np.ndarray, path: Path) -> None:
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
    logging.info(f"Saved class balance plot -> {path}")
