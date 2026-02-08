import logging
import pandas as pd
from src.core import Handler, PipelineContext

class FilterITRolesHandler(Handler):
    """
    Handler for filtering the dataset to include only IT-related roles.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"FilterITRolesHandler: Filtering for IT roles")
        df = ctx.df.copy()
        
        keywords = [
            'developer', 'разработчик', 'programmer', 'программист', 
            'engineer', 'инженер', 'data scientist', 'data analyst', 
            'qa', 'tester', 'тестировщик', 'devops', 'admin', 'администратор',
            'backend', 'frontend', 'fullstack', 'python', 'java', 'c++', 'golang'
        ]
        
        def is_it_role(value: str) -> bool:
            if not isinstance(value, str):
                return False
            val_lower = value.lower()
            return any(k in val_lower for k in keywords)

        initial_count = len(df)
        df = df[df['Ищет работу на должность:'].apply(is_it_role)]
        
        logging.info(f"FilterITRolesHandler: Filtered from {initial_count} to {len(df)} rows")
        
        ctx.df = df
        return ctx
