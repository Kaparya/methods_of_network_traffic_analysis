import logging
from src.core import Handler, PipelineContext

class ParseCityHandler(Handler):
    """Handler for parsing city information."""
    def _process(self, ctx: PipelineContext) -> PipelineContext:
        """
        Group cities into regions/federal districts.

        Args:
            ctx: Pipeline context containing the dataframe.

        Returns:
            PipelineContext: Context updated with 'city' region column.
        """
        logging.info("ParseCityHandler: Starting to parse city")
        dataframe = ctx.dataframe.copy()

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
        logging.info("ParseCityHandler: Parsed city")
        return ctx
