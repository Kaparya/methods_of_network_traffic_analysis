from src.core import Handler, PipelineContext

import logging

class ParseCityHandler(Handler):
    """
    Handler for parsing city information from the "Город" column.

    Methods:
        _process(ctx): Extracts new columns for city from the raw text column.
    """
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        logging.info(f"ParseCityHandler: Starting to parse city")
        dataframe = ctx.dataframe.copy()

        # group cities by regions
        regions_map = {
            "Moscow & Oblast": [
                "Москва", "Moscow", "Зеленоград", "Подольск", "Балашиха", "Химки", "Мытищи", 
                "Королев", "Люберцы", "Красногорск", "Одинцово", "Домодедово", "Щелково", 
                "Серпухов", "Раменское", "Долгопрудный", "Реутов", "Пушкино", "Лобня"
            ],
            "Saint Petersburg & Oblast": [
                "Санкт-Петербург", "Saint Petersburg", "Гатчина", "Выборг", "Всеволожск", 
                "Сосновый Бор", "Кириши", "Тихвин", "Сертолово"
            ],
            "Central Federal District": [
                "Воронеж", "Ярославль", "Рязань", "Тверь", "Тула", "Липецк", "Курск", 
                "Брянск", "Иваново", "Белгород", "Владимир", "Калуга", "Орел", "Смоленск", 
                "Тамбов", "Кострома", "Старый Оскол"
            ],
            "Volga Federal District": [
                "Казань", "Kazan", "Нижний Новгород", "Самара", "Уфа", "Пермь", "Саратов", 
                "Тольятти", "Ижевск", "Ульяновск", "Оренбург", "Пенза", "Набережные Челны", 
                "Чебоксары", "Киров", "Саранск", "Стерлитамак", "Йошкар-Ола"
            ],
            "South and North Caucasus Federal District": [
                "Краснодар", "Ростов-на-Дону", "Волгоград", "Сочи", "Ставрополь", "Астрахань", 
                "Севастополь", "Симферополь", "Новороссийск", "Таганрог", "Махачкала", 
                "Владикавказ", "Грозный", "Майкоп", "Пятигорск"
            ],
            "Ural Federal District": [
                "Екатеринбург", "Yekaterinburg", "Челябинск", "Тюмень", "Магнитогорск", 
                "Сургут", "Нижневартовск", "Курган", "Новый Уренгой", "Ноябрьск", "Ханты-Мансийск"
            ],
            "Siberian Federal District": [
                "Новосибирск", "Novosibirsk", "Красноярск", "Омск", "Томск", "Барнаул", 
                "Иркутск", "Кемерово", "Новокузнецк", "Абакан", "Братск", "Ангарск"
            ],
            "Far Eastern Federal District": [
                "Владивосток", "Хабаровск", "Улан-Удэ", "Чита", "Благовещенск", "Якутск", 
                "Петропавловск-Камчатский", "Южно-Сахалинск", "Находка"
            ],
            "Kazakhstan": [
                "Алматы", "Almaty", "Нур-Султан", "Астана", "Astana", "Шымкент", "Актобе", 
                "Караганда", "Атырау", "Актау", "Павлодар", "Уральск"
            ],
            "Belarus": [
                "Минск", "Minsk", "Гомель", "Витебск", "Могилев", "Гродно", "Брест"
            ],
            "Other countries / CIS": [
                "Киев", "Kyiv", "Ташкент", "Бишкек", "Тбилиси", "Баку", "Ереван", "Рига", "Вильнюс"
            ]
        }

        def extract_city(value: str) -> str:
            city_name = value.split(',')[0].strip()
            for region, cities in regions_map.items():
                if city_name in cities:
                    return region
            return "Other"

        dataframe["city"] = dataframe["Город"].apply(extract_city)

        dataframe = dataframe.drop(columns=["Город"])

        ctx.dataframe = dataframe
        logging.info(f"ParseCityHandler: Parsed city")
        return ctx
