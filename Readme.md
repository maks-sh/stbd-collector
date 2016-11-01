Репозиторий для курса "Специальные технологии баз данных и экспертных систем"
=========================
Используемая версия Python: 3.5.2

Назначение программы
----------------------------
Программа предназначена для автоматического сбора информации для проведения дальнейшего ее анализа с целью выявления банков, у которых в будущем времени отзовут лицензию.

С открытых источников <http://www.banki.ru/> и <http://bankir.ru/> собирается информация о показателях банка, которые упоминаются в [статье Евстефеевой, Крылова и Рябкова](./article.pdf).

Так как в отрытых источниках не было найдено ни одного сайта, где можно было бы экспортировать в удобном формате информацию об отозванных банков, был написан метод parser() класса Table, который парсит эти данные [отсюда](http://www.banki.ru/banks/memory/)