from src.core import Handler, PipelineContext

import logging

class ParseSalaryHandler(Handler):
    """
    Handler for parsing salary information from the "ЗП" column and converting it to rubles.

    Methods:
        _process(ctx): Extracts new columns for salary from the raw text column and converts it to rubles.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseSalaryHandler: Starting to parse salary")
        dataframe = ctx.dataframe.copy()

        # approximate 2020 currency rates
        currency_rates = {
            'руб.': 1.0,
            'USD': 73.35,
            'RUB': 1.0,
            'KZT': 0.18,
            'бел. руб.': 2.28,
            'EUR': 85.86,
            'грн.': 2.72,
            'сум': 0.005,
            'KGS': 0.98,
            'UAH': 2.5,
            'BYN': 2.5,
            'AZN': 41.1,
            'som': 0.005,
        }
        
        def extract_salary(value: str) -> str:
            value = value.replace('\xa0', ' ').strip().split(' ')
            number = ''
            currency = ''

            for idx, cur in enumerate(value):
                if cur.isdigit():
                    number += cur
                else:
                    currency = ' '.join(value[idx:])
                    break
            return currency_rates[currency.strip()] * float(number)

        dataframe["salary_rub"] = dataframe["ЗП"].apply(extract_salary)

        dataframe = dataframe.drop(columns=["ЗП"])

        ctx.dataframe = dataframe
        logging.info(f"ParseSalaryHandler: Parsed salary")
        return ctx
