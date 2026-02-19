from src.core import Handler
from src.handlers.io import LoadCSVHandler
from src.handlers.filtering import FilterITRolesHandler
from src.handlers.labeling import LabelGradeHandler
from src.handlers.parsing import (
    ParseGenderAgeBirthdayHandler, ParseSalaryHandler,
    ParseCityHandler, ParseEmploymentHandler, ParseWorkScheduleHandler,
    ParseExperienceHandler, ParseLastPlaceHandler,
    ParseEducationHandler, ParseResumeHandler, ParseAutoHandler, ParseEperienceNLPHandler
)
from src.handlers.preprocessing import (
    EncodeCategoricalFeaturesHandler, SplitClassificationDataHandler
)

def build_pipeline() -> Handler:
    """
    Builds the full data processing pipeline by chaining together all handlers in the required order.

    Returns:
        Handler: The first handler in the pipeline (LoadCSVHandler).
    """
    load = LoadCSVHandler()
    it_filter = FilterITRolesHandler()
    gender_age = ParseGenderAgeBirthdayHandler()
    salary = ParseSalaryHandler()
    city = ParseCityHandler()
    employment = ParseEmploymentHandler()
    work_schedule = ParseWorkScheduleHandler()
    experience = ParseExperienceHandler()
    experience_nlp = ParseEperienceNLPHandler()
    grade = LabelGradeHandler()
    last_place = ParseLastPlaceHandler()
    education = ParseEducationHandler()
    resume = ParseResumeHandler()
    auto = ParseAutoHandler()
    encode_categorical_features = EncodeCategoricalFeaturesHandler()

    split_data = SplitClassificationDataHandler()

    load.set_next(it_filter)\
        .set_next(gender_age)\
        .set_next(salary)\
        .set_next(experience)\
        .set_next(experience_nlp)\
        .set_next(grade)\
        .set_next(city)\
        .set_next(employment)\
        .set_next(work_schedule)\
        .set_next(last_place)\
        .set_next(education)\
        .set_next(resume)\
        .set_next(auto)\
        .set_next(encode_categorical_features)\
        .set_next(split_data)
    
    return load
