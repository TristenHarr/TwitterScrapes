import pandas as pd
import sqlite3 as sq


class DataBaseSearch(object):
    def __init__(self, database):
        self.database = "../ScrapeData/DATABASE/{}.sqlite".format(database)
        self.con = sq.connect(self.database)

    def get_DataFrame(self, query):
        my_frame = pd.read_sql_query("{}".format(query), self.con)
        return my_frame


class Table(object):

    def __init__(self, table=None, rows=list(), alias=""):
        """
        :type table: str
        :param table: Table name
        :type rows: list
        :param rows: Table rows to retrieve
        :type alias: str
        :param alias: Table alias
        """
        self.table = str(table)
        self.table_show = str(table)
        self.rows = "{}".format(",".join(rows))
        self.alias = alias
        self.statement_list = []
        self.query = "SELECT {} FROM {} {} ".format(self.rows, self.table, self.alias)

    def __str__(self):
        return 'Current Table: {}'.format(self.table_show)

    def update(self):
        self.query = 'SELECT {} FROM {} {} '.format(self.rows, self.table, self.alias)
        for item in self.statement_list:
            self.query += item

    def set_table(self, table):
        self.table_show = "{}".format(table)
        self.table = table
        self.update()

    def select_rows(self, rows=list()):
        for item in rows:
            self.rows += "{},".format(item)
        else:
            self.rows = self.rows.strip(',')
        # self.query = "SELECT {} FROM {} {} {}".format(self.rows, self.table, self.alias, self.statement)
        self.update()

    def add_rows(self, rows=list()):
        for item in rows:
            self.rows += "{}".format(item)

    def set_alias(self, alias):
        self.alias = "AS {}".format(alias)
        self.update()
        # self.query = "SELECT {} FROM {} {} {}".format(self.rows, self.table, self.alias, self.statement)

    def commit_statement(self, statement):
        self.query = self.query.strip(';" ') + " "
        self.query += str(statement) + ';'


class Statement(object):
    def __init__(self, choice_type="WHERE", selection="", comparison="LIKE", parameters=""):
        self._choice_type = choice_type
        self._selection = selection
        self._operator = comparison
        self._condition = parameters

    def set_selection(self, selection):
        self._selection = selection

    def set_comparison(self, comparison='LIKE'):
        self._operator = comparison

    def set_parameters(self, parameters="%Stuff%"):
        self._condition = parameters

    def __str__(self):
        return "{} {} {} {}".format(self._choice_type, self._selection, self._operator, self._condition)
