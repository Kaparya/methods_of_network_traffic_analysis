import sys
import logging
import traceback
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from catboost import CatBoostClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from src.core import PipelineContext, Handler
from src.handlers.filtering import FilterITRolesHandler
from src.handlers.io import LoadCSVHandler
from src.handlers.labeling import LabelGradeHandler
from src.handlers.parsing import (
    ParseAutoHandler,
    ParseCityHandler,
    ParseEducationHandler,
    ParseEmploymentHandler,
    ParseExperienceHandler,
    ParseGenderAgeBirthdayHandler,
    ParseJobHandler,
    ParseLastJobHandler,
    ParseLastPlaceHandler,
    ParseResumeHandler,
    ParseSalaryHandler,
    ParseWorkScheduleHandler,
)
from src.handlers.preprocessing import (
    EncodeCategoricalFeaturesHandler,
    SplitClassificationDataHandler,
)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    input_path = Path("parsing/hh.csv")
    if not input_path.exists():
        input_path = Path("../parsing/hh.csv")
        if not input_path.exists():
            input_path = Path("hh.csv")
            
    if not input_path.exists():
        print("Error: hh.csv not found.")
        sys.exit(1)

    pipeline = LoadCSVHandler()
    
    pipeline.set_next(FilterITRolesHandler())\
            .set_next(ParseGenderAgeBirthdayHandler())\
            .set_next(ParseSalaryHandler())\
            .set_next(ParseExperienceHandler())\
            .set_next(LabelGradeHandler())\
            .set_next(ParseJobHandler())\
            .set_next(ParseCityHandler())\
            .set_next(ParseEmploymentHandler())\
            .set_next(ParseWorkScheduleHandler())\
            .set_next(ParseLastPlaceHandler())\
            .set_next(ParseLastJobHandler())\
            .set_next(ParseEducationHandler())\
            .set_next(ParseResumeHandler())\
            .set_next(ParseAutoHandler())\
            .set_next(EncodeCategoricalFeaturesHandler())\
            .set_next(SplitClassificationDataHandler())

    print("Starting pipeline execution...")
    ctx = PipelineContext(csv_path=input_path)
    try:
        ctx = pipeline.handle(ctx)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    X = ctx.X
    y = ctx.y
    feature_names = getattr(ctx, 'feature_names', [f"feat_{i}" for i in range(X.shape[1])])
    
    print(f"Data ready. X shape: {X.shape}, y shape: {y.shape}")
    
    # 4. Analyze Class Balance
    unique, counts = np.unique(y, return_counts=True)
    print("\nClass Balance:")
    for label, count in zip(unique, counts):
        print(f"{label}: {count} ({count/len(y)*100:.2f}%)")
        
    # Plot balance
    try:
        plt.figure(figsize=(8, 6))
        plt.bar(unique, counts)
        plt.title("Distribution of Developer Grades")
        plt.xlabel("Grade")
        plt.ylabel("Count")
        plt.savefig("grade_distribution.png")
        print("Saved grade_distribution.png")
    except Exception as e:
        print(f"Failed to plot: {e}")

    # 5. Encode Target
    le = LabelEncoder()
    y_encoded = le.fit_transform(y)
    
    # 6. Train/Test Split
    # Reduce dataset size for PoC to prevent OOM
    print(f"Original dataset size: {X.shape[0]}")
    sample_size = min(50, X.shape[0])  # Extremely small sample
    indices = np.random.choice(X.shape[0], sample_size, replace=False)
    X_sample = X[indices]
    y_encoded_sample = y_encoded[indices]
    print(f"Sampled dataset size: {X_sample.shape[0]}")
    
    X_train, X_test, y_train, y_test = train_test_split(X_sample, y_encoded_sample, test_size=0.2, random_state=42, stratify=y_encoded_sample)
    
    # 7. Model Training
    print("\nTraining CatBoost Classifier...")
    clf = CatBoostClassifier(
        iterations=10, 
        learning_rate=0.1, 
        depth=2, 
        loss_function='MultiClass',
        random_seed=42,
        verbose=False,
        allow_writing_files=False,
        thread_count=1
    )
    clf.fit(X_train, y_train)
    
    # 8. Evaluation
    print("\nEvaluating model...")
    y_pred = clf.predict(X_test)
    
    print("\nClassification Report:")
    report = classification_report(y_test, y_pred, target_names=le.classes_)
    print(report)
    
    # Feature Importance
    try:
        importances = clf.feature_importances_
        indices = np.argsort(importances)[::-1]
        
        print("\nTop 10 Important Features:")
        for feature_index in range(min(10, len(feature_names))):
            print(f"{feature_index+1}. {feature_names[indices[feature_index]]}: {importances[indices[feature_index]]:.4f}")
    except Exception as e:
        print(f"Could not print feature importance: {e}")

    # Save metrics to a file for review
    with open("classification_report.txt", "w") as f:
        f.write("Classification Report\n")
        f.write("=====================\n\n")
        f.write(report)
        f.write("\n\nTop 10 Features:\n")
        try:
            for feature_index in range(min(10, len(feature_names))):
                f.write(f"{feature_names[indices[feature_index]]}: {importances[indices[feature_index]]:.4f}\n")
        except:
            pass

if __name__ == "__main__":
    main()
