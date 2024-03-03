import pandas as pd
from sqlalchemy import select, column, table, insert

from db_connection import get_session


class ParseExcelToDB:
    def __init__(self):
        self.session = get_session()

    @staticmethod
    def get_excel_data() -> tuple[dict, int]:
        dataframe = pd.read_excel("ФСД дз4.xlsx")
        return dataframe.to_dict(), len(dataframe)

    def check_tariff(self, tariffs) -> bool | tuple[bool, str]:
        query = select(column("name")).select_from(table("Tariff"))
        result = self.session.execute(query)
        db_tariffs = [row for row in result.scalars().all()]
        for t in list(tariffs):
            if str(t) not in db_tariffs:
                return False, f"Тариф {t} не существует"
        return True

    def insert_call(self, data, data_len):
        for i in range(data_len):
            caller_id = self.session.execute(
                select(column("id"))
                .select_from(table("Client"))
                .where(column("number") == str(data["Вызывающий"][i])[1:])
            )
            called_id = self.session.execute(
                select(column("id"))
                .select_from(table("Client"))
                .where(column("number") == str(data["Вызываемый"][i])[1:])
            )
            self.session.execute(
                table("Call")
                .insert()
                .values(
                    **{
                        "id": i + 1,
                        "caller_id": caller_id.all()[0][0],
                        "called_id": called_id.all()[0][0],
                        "status": data["Статус"][i],
                        "duration": data["Длительность"][i],
                        "call_date": data["timestamp"][i],
                        "redirected_to": None,
                    }
                    # id=i + 1,
                    # caller_id=caller_id.all()[0][0],
                    # called_id=called_id.all()[0][0],
                    # status=data["Статус"][i],
                    # duration=data["Длительность"][i],
                    # call_date=data["timestamp"][i],
                    # redirected_to=None,
                )
            )
        self.session.commit()

    def write_calls_to_db(self):
        data, data_len = self.get_excel_data()
        tariff_check = self.check_tariff(set(data["Тариф"].values()))
        if isinstance(tariff_check, tuple) and not tariff_check[0]:
            raise Exception(f"Тариф не существует")

        self.insert_call(data, data_len)
        self.session.close()

    def fill_data_mart(self):
        pass


# ParseExcelToDB().check_tariff()
ParseExcelToDB().write_calls_to_db()
