from src.core import Handler
from src.handlers import (
    LoadCSVHandler, ParseGenderHandler, ParseAgeHandler, ParseBirthdayMonthHandler, ParseSalaryHandler,
    ParseJobHandler, ParseCityHandler, ParseEmploymentHandler, ParseWorkScheduleHandler,
    ParseExperienceHandler, ParseLastPlaceHandler, ParseLastJobHandler, ParseEducationHandler,
    ParseResumeHandler, ParseAutoHandler, EncodeCategoricalFeaturesHandler, SplitDataHandler, SaveDataHandler
)

def build_pipeline() -> Handler:
    """
    Builds the full data processing pipeline by chaining together all handlers in the required order.

    Returns:
        Handler: The first handler in the pipeline (LoadCSVHandler).
    """
    load = LoadCSVHandler()
    gender = ParseGenderHandler()
    age = ParseAgeHandler()
    birthday_month = ParseBirthdayMonthHandler()
    salary = ParseSalaryHandler()
    job = ParseJobHandler()
    city = ParseCityHandler()
    employment = ParseEmploymentHandler()
    work_schedule = ParseWorkScheduleHandler()
    experience = ParseExperienceHandler()
    last_place = ParseLastPlaceHandler()
    last_job = ParseLastJobHandler()
    education = ParseEducationHandler()
    resume = ParseResumeHandler()
    auto = ParseAutoHandler()
    encode_categorical_features = EncodeCategoricalFeaturesHandler()

    split_data = SplitDataHandler()

    save_data = SaveDataHandler()

    load.set_next(gender)\
        .set_next(age)\
        .set_next(birthday_month)\
        .set_next(salary)\
        .set_next(job)\
        .set_next(city)\
        .set_next(employment)\
        .set_next(work_schedule)\
        .set_next(experience)\
        .set_next(last_place)\
        .set_next(last_job)\
        .set_next(education)\
        .set_next(resume)\
        .set_next(auto)\
        .set_next(encode_categorical_features)\
        .set_next(split_data)\
        .set_next(save_data)
    
    return load
