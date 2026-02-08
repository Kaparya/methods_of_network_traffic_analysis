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
            title = str(row.get('job_title_temp', '')).lower()
            exp_months = row.get('experience_months', 0)
            
            # Keywords for specific grades
            junior_keywords = ['junior', 'trainee', 'stager', 'intern', 'младший', 'стажер', 'начальный', 'assistant', 'ассистент']
            senior_keywords = ['senior', 'lead', 'principal', 'ведущий', 'главный', 'руководитель', 'head', 'architect', 'expert', 'эксперт', 'mentor']
            middle_keywords = ['middle', 'мидл'] # Less common in titles, usually implied by absence of Junior/Senior
            
            # 1. Check title for explicit grade
            is_junior = any(k in title for k in junior_keywords)
            is_senior = any(k in title for k in senior_keywords)
            is_middle = any(k in title for k in middle_keywords)
            
            # Conflict resolution: if both (e.g. "Senior Assistant"), trust Senior if exp is high, else Junior
            if is_senior and is_junior:
                if exp_months > 36:
                    return 'Senior'
                else:
                    return 'Junior'
            
            if is_senior:
                return 'Senior'
            if is_junior:
                return 'Junior'
            if is_middle:
                return 'Middle'

            # 2. Experience based fallback (adjusted thresholds)
            # < 1.5 years (18 months) -> Junior
            # 1.5 - 3 years (18 - 36 months) -> Junior+ / Middle- (let's map to Middle for now to balance classes, or keep Junior strict)
            # Let's relax Junior threshold slightly to capture "strong juniors"
            
            if exp_months <= 12: # Strictly Junior
                return 'Junior'
            elif exp_months <= 24: # 1-2 years
                # Often considered Junior, but could be Middle in some stacks. 
                # Let's keep it Junior to avoid polluting Middle class with beginners.
                return 'Junior'
                
            elif exp_months > 60: # > 5 years -> Senior
                return 'Senior'
            
            # Everything else (2 - 5 years) -> Middle
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
