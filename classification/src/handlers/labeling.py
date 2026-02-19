import logging

from src.core import Handler, PipelineContext

class LabelGradeHandler(Handler):
    """
    Generates target variable 'grade' (Junior/Middle/Senior) using title and experience,
    then drops these features to prevent target leakage.
    """

    _JUNIOR_KW = ['junior', 'jun ', 'jr ', 'trainee', 'стажер', 'стажёр', 'intern', 'младший', 'начинающий']
    _SENIOR_KW = ['senior', 'sr ', 'lead', 'principal', 'staff', 'ведущий', 'главный', 'руководитель', 'team lead', 'architect', 'head of', 'expert']
    _MIDDLE_KW = ['middle', 'mid ', 'мидл', 'мидлл']

    def label_grade(self, row) -> str:
        """Determine grade based on title keywords and experience."""
        title: str = row['_title']
        exp: int = row.get('experience_months', 0)

        # Keyword-based classification
        if any(kw in title for kw in self._SENIOR_KW):
            # Resolve Junior/Senior ambiguity if both keywords present
            if any(kw in title for kw in self._JUNIOR_KW):
                return 'Senior' if exp > 36 else 'Junior'
            return 'Senior'
            
        if any(kw in title for kw in self._JUNIOR_KW):
            return 'Middle' if exp > 60 else 'Junior'
            
        if any(kw in title for kw in self._MIDDLE_KW):
            return 'Senior' if exp > 96 else 'Middle'

        # Experience-based classification for no keywords
        if exp <= 18:
            return 'Junior'
        if exp <= 60:
            return 'Middle'
        return 'Senior'

    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Generate 'grade' target and remove source features.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with 'grade' target and clean feature set.
        """
        logging.info("LabelGradeHandler: Starting to label grades")
        dataframe = ctx.dataframe.copy()

        raw_title_col = 'Ищет работу на должность:'
        if raw_title_col in dataframe.columns:
            dataframe['_title'] = dataframe[raw_title_col].fillna('').str.lower()
        elif 'job' in dataframe.columns:
            dataframe['_title'] = dataframe['job'].fillna('').str.lower()
        else:
            dataframe['_title'] = ''

        dataframe['grade'] = dataframe.apply(self.label_grade, axis=1)

        # Drop features used for labeling to prevent leakage
        cols_to_drop = ['_title']
        if raw_title_col in dataframe.columns: cols_to_drop.append(raw_title_col)
        if 'experience_months' in dataframe.columns: cols_to_drop.append('experience_months')
        if 'Последеняя/нынешняя должность' in dataframe.columns: cols_to_drop.append('Последеняя/нынешняя должность')

        dataframe = dataframe.drop(columns=cols_to_drop)

        ctx.dataframe = dataframe
        ctx.target = dataframe['grade'].values

        logging.info(f"LabelGradeHandler: Distribution:\n{dataframe['grade'].value_counts().to_string()}")
        logging.info(f"LabelGradeHandler: Dropped {cols_to_drop} to prevent leakage.")
        return ctx
