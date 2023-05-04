import re
import uuid

import pandas as pd
import sqlparse
from django.db import connection


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


# a function which checks db for already present indexes
def already_present_indexes():
    cursor = connection.cursor()
    cursor.execute("""
                    select
                    t.relname as table_name,
                    i.relname as index_name,
                    a.attname as column_name
                from
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a
                where
                    t.oid = ix.indrelid
                    and i.oid = ix.indexrelid
                    and a.attrelid = t.oid
                    and a.attnum = ANY(ix.indkey)
                    and t.relkind = 'r'
                   -- and t.relname like 'mytable'
                order by
                    t.relname,
                    i.relname;
    """)

    index_info = dictfetchall(cursor)
    cursor.close()

    return index_info


def check_for_indexing(table_name, column_name):
    try:
        present_index_info = already_present_indexes()
        index_name = f'{table_name}_{column_name}_idx'

        for row in present_index_info:
            if row['index_name'] == index_name.lower():
                return

        cursor = connection.cursor()
        cursor.execute(f'SELECT DISTINCT pg_typeof("{column_name}") FROM "logSQL".public."{table_name}"')
        type_of_value = dictfetchall(cursor)[0]["pg_typeof"]

        cursor.execute(
            f'SELECT count(*) <> count(distinct"{column_name}") as duplicate_flag FROM "logSQL".public."{table_name}"')
        is_duplicate = dictfetchall(cursor)[0]["duplicate_flag"]

        cursor.close()

        if not is_duplicate:
            create_unique_index(table_name, column_name)
        elif type_of_value == "integer":
            create_brin_index(table_name, column_name)
        elif type_of_value == "text":
            create_btree_index(table_name, column_name)
        else:
            create_non_clustered_index(table_name, column_name)

    except Exception as e:
        print(e)
    return


def check_for_indexing_multiple_columns(table_name, column_names):
    try:
        present_index_info = already_present_indexes()
        column_names.sort()
        index_column_name = '_'.join(column_names)
        index_name = f'{table_name}_{index_column_name}_idx'

        for row in present_index_info:
            if row['index_name'] == index_name.lower():
                return

        with connection.cursor() as cursor:
            query_column_name = ','.join(column_names)
            cursor.execute(
                f'CREATE INDEX {index_name} ON "logSQL".public."{table_name}"({query_column_name})')

        cursor.close()
    except Exception as e:
        print(e)
    return


def check_for_indexing_multiple_columns_with_condition_check(table_name, column_names, where_clause):
    cursor = connection.cursor()

    for col in column_names:
        cursor.execute(f'SELECT DISTINCT pg_typeof("{col}") FROM "logSQL".public."{table_name}"')
        type_of_value = dictfetchall(cursor)[0]["pg_typeof"]

        if type_of_value != "integer" and type_of_value != "bigint":
            return

    try:
        column_names.sort()
        index_column_name = '_'.join(column_names)
        index_name = f'{table_name}_{index_column_name}_idx_' + str(uuid.uuid4())[0:8]
        where_clause = where_clause.replace('\n', ' ')

        with connection.cursor() as cursor:
            query_column_name = ','.join(column_names)
            print(type(where_clause))
            print(where_clause)
            index_query = f'CREATE INDEX {index_name} ON "logSQL".public."{table_name}"({query_column_name}) {where_clause}'
            print(index_query)

            cursor.execute(
                f"""SELECT EXISTS (SELECT indexdef FROM "logSQL".pg_catalog.pg_indexes WHERE indexdef = '{index_query}')""")
            value = dictfetchall(cursor)[0]["exists"]
            if not value:
                cursor.execute(index_query)

        cursor.close()
    except Exception as e:
        print(e)
    return


# there is no clustered index in postgres default is non clustered
def create_non_clustered_index(table_name, column_name):
    with connection.cursor() as cursor:
        index_name = f'{table_name}_{column_name}_idx'
        cursor.execute(
            f'CREATE INDEX {index_name} ON "logSQL".public."{table_name}"({column_name})')


# Speeds up queries for large tables
# create brin index in postgres sql
def create_brin_index(table_name, column_name, *args):
    with connection.cursor() as cursor:
        index_name = f'{table_name}_{column_name}_idx'

        cursor.execute(
            f'CREATE INDEX {index_name} ON "logSQL".public."{table_name}" USING BRIN({column_name})')


# create btree index in postgresql
def create_btree_index(table_name, column_name):
    with connection.cursor() as cursor:
        index_name = f'{table_name}_{column_name}_idx'
        cursor.execute(
            f'CREATE INDEX {index_name} ON "logSQL".public."{table_name}" USING BTREE({column_name})')


# crate unique index in postgresql
def create_unique_index(table_name, column_name):
    with connection.cursor() as cursor:
        index_name = f'{table_name}_{column_name}_idx'
        cursor.execute(
            f'CREATE UNIQUE INDEX {index_name} ON "logSQL".public."{table_name}"({column_name})')


def read_file(file_path):
    allselectstatements = []
    with open("E:\Projects\IDS Proj\indexingProj\debug.log", 'r') as f:
        x = f.read()
        for line in re.split(r'\([0-9]+\.[0-9]+\)', x):
            allselectstatements.append(line.strip())

    return allselectstatements


def get_dataframe(select_queries):
    table_name = ''
    where_clause = ''

    query_number_list = []
    table_name_list = []
    column_name_list = []
    where_clauses_list = []
    separate_clauses = []

    for (j, query) in enumerate(select_queries):
        token_list = sqlparse.parse(sqlparse.split(query)[0])[0].tokens

        for (i, v) in enumerate(token_list):
            if v.ttype == sqlparse.tokens.Keyword and v.value == "FROM":
                table_name = token_list[i + 2].value
                table_name = table_name.strip('\"')

            if table_name != '' and table_name.startswith("logSQL"):
                if v.value.startswith("WHERE"):
                    where_clause = v.value.split(";")[0]
                    clauses = where_clause.strip("WHERE").strip(' ').split("AND")
                    for clause in clauses:
                        try:
                            column_name = clause.split(table_name)[1].split('.')[1].split('"')[1]
                            query_number_list.append(j)
                            table_name_list.append(table_name)
                            column_name_list.append(column_name)
                            where_clauses_list.append(where_clause)
                            separate_clauses.append(clause)
                        except:
                            pass

    sql_data = {
        'query_no': query_number_list,
        'table_name': table_name_list,
        'column_name': column_name_list,
        'where_clause': where_clauses_list,
        'separate_clause': separate_clauses

    }

    df = pd.DataFrame(data=sql_data)
    return df


def main_runner():
    # check_for_indexing("logSQL_student", "age")

    n_list = read_file("E:\Projects\IDS Proj\indexingProj\debug.log")
    select_queries = [x for x in n_list if x.startswith("SELECT")]
    df_select = get_dataframe(select_queries)

    # check df_select and check what type of indexing is best for the column
    for index, row in df_select.iterrows():
        check_for_indexing(row['table_name'], row['column_name'])

    # return all rows in df_select query_no column who are common
    for query_no in df_select['query_no'].unique():
        df = df_select[df_select['query_no'] == query_no]
        c_names = df['column_name'].unique()
        t_names = df['table_name'].unique()
        check_for_indexing_multiple_columns(t_names[0], c_names)

    # adds indexing with where clasue
    for query_no in df_select['query_no'].unique():
        df = df_select[df_select['query_no'] == query_no]
        c_names = df['column_name'].unique()
        t_names = df['table_name'].unique()
        check_for_indexing_multiple_columns_with_condition_check(t_names[0], c_names, df.iloc[0]["where_clause"])
