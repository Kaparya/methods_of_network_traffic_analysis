import logging
import pandas as pd
from src.core import Handler, PipelineContext

class LabelGradeHandler(Handler):
    """
    Handler for generating the target variable 'grade' (Junior, Middle, Senior).
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"LabelGradeHandler: Starting to label grades")
        df = ctx.df.copy()

        def get_grade(row) -> str:
            # Normalize strings
            title = str(row.get('job_title', '')).lower()
            # If job_title column doesn't exist (it was dropped or named differently), try to find it
            # In our pipeline 'Ищет работу на должность:' was mapped to 'job' (top 133) or 'other'.
            # We might need the raw title for better labeling.
            # But the 'ParseJobHandler' drops the original column. 
            # We should probably insert this handler BEFORE ParseJobHandler drops the column, 
            # or rely on 'experience_months'.
            
            # Wait, the current pipeline drops 'Ищет работу на должность:'.
            # We need to ensure we have access to the original title or experience.
            # 'experience_months' is created by ParseExperienceHandler.
            
            exp_months = row.get('experience_months', 0)
            
            # Heuristic rules
            if 'junior' in title or 'trainee' in title or 'stager' in title or 'младший' in title:
                return 'Junior'
            if 'senior' in title or 'lead' in title or 'principal' in title or 'ведущий' in title:
                return 'Senior'
            
            # Experience based fallback
            if exp_months < 18: # < 1.5 years
                return 'Junior'
            if exp_months > 60: # > 5 years
                return 'Senior'
                
            return 'Middle'

        # We need to check if we have the necessary columns.
        # Ideally, we run this after Experience parsing but before dropping Title (if we want to use Title).
        # However, the current ParseJobHandler drops the title.
        # For the PoC, let's assume we insert this handler at the right place.
        # But wait, the handlers in `src` are fixed. 
        # I will handle the column availability in the script by modifying the pipeline order 
        # or assuming the 'job' column (which is top 133) is not enough.
        
        # Actually, let's look at ParseJobHandler. It creates 'job' column and drops 'Ищет работу на должность:'.
        # The 'job' column only has top 133 standardized names.
        # We might miss "Junior Java Developer" if it's not in top 133.
        # So we probably need to parse the grade from the RAW column before it's dropped.
        
        # Challenge: The existing handlers modify df in place and drop columns.
        # Solution: I will modify the ParseJobHandler to NOT drop the column immediately, 
        # or I will Create a custom pipeline for the PoC that uses a slightly different JobHandler 
        # or I will modify `src/handlers/parsing.py` to be more flexible.
        
        # For this task, I will add a method to LabelGradeHandler that assumes 'Ищет работу на должность:' exists.
        
        target_col = 'grade'
        if 'Ищет работу на должность:' in df.columns:
            # We have the raw title
            df['job_title_temp'] = df['Ищет работу на должность:']
        elif 'job' in df.columns:
            # We only have the simplified job
            df['job_title_temp'] = df['job']
        else:
            df['job_title_temp'] = ''
            
        df[target_col] = df.apply(get_grade, axis=1)
        df = df.drop(columns=['job_title_temp'])

        ctx.df = df
        
        # Update target in context
        ctx.y = df[target_col].values
        
        logging.info(f"LabelGradeHandler: Labeled grades. Distribution:\n{df[target_col].value_counts()}")
        return ctx
