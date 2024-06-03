
class data_access():
    def __init__(self, session) -> None:
        self.session = session

    def get_table(self, table_name: str):
        df_table = self.session.table(table_name)
        return df_table
