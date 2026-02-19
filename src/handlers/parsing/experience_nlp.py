import logging
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from src.core import Handler, PipelineContext

class ParseEperienceNLPHandler(Handler):
    """Handler for NLP processing of experience description."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Extract TF-IDF features from experience description.
        
        Args:
            ctx: Pipeline context containing the dataframe.
            
        Returns:
            PipelineContext: Context updated with TF-IDF features.
        """
        logging.info("ParseDescriptionNLPHandler: Starting NLP processing")
        dataframe = ctx.dataframe.copy()
        
        text_col = 'Опыт (двойное нажатие для полной версии)'
            
        if text_col not in dataframe.columns:
            logging.warning(f"ParseDescriptionNLPHandler: Column '{text_col}' not found. Skipping NLP.")
            return ctx
        
        tfidf = TfidfVectorizer(
            max_features=50,
            ngram_range=(1, 2),
            binary=True
        )

        def extract_text(value: str) -> str:
            text = ''
            try:
                possible_strings = ['месяц', 'год', 'лет']
                drop_index = -1
                for current in possible_strings:
                    drop_index = max(value.find(current) + len(current), drop_index)
                if drop_index != -1:
                    text = value[drop_index + 2:] # +2 accounts for skip double '\n\n'
            except Exception as error:
                logging.info(error)
            return text
        
        texts = dataframe[text_col].apply(extract_text).astype(str)
        
        try:
            tfidf_matrix = tfidf.fit_transform(texts)
            feature_names = [f"tfidf_{name}" for name in tfidf.get_feature_names_out()]
            
            tfidf_df = pd.DataFrame(
                tfidf_matrix.toarray(), 
                columns=feature_names, 
                index=dataframe.index
            )
            
            dataframe = pd.concat([dataframe, tfidf_df], axis=1)
            logging.info(f"ParseDescriptionNLPHandler: Added {len(feature_names)} TF-IDF features")
            
        except Exception as e:
            logging.error(f"ParseDescriptionNLPHandler: NLP failed: {e}")
            
        dataframe = dataframe.drop(columns=[text_col])
        ctx.dataframe = dataframe
        return ctx
