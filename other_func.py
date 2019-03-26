import sys
import re
import csv

class other_func:
    def __init__(self):
        pass

    def format_string(self, string):
        string2 = ' +'
        return (re.sub(string2, ' ', string)).strip()

    def read_table_data(self, table_name):
        data = []
        flag = True
        file_name = table_name + '.csv'

        try:
            f = open(file_name, 'rb')
            reader = csv.reader(f)
            for row in reader:
                data.append(row)
            if flag:
                pass
            f.close()
        except IOError:
            flag = False
            sys.exit('No file was provided for the given table: \'' + table_name + '\'')
        return data

    def print_header(self,table_name, columns):
        string = ''
        comma = ','
        for column in columns:
            if string != '':
                string += comma
            temp_name = table_name + '.' + column
            string += temp_name
        return string

    def search_col(self, column, tables, dict):
        flag = True
        if '.' in column:
            table, column = column.split('.')
            table = self.format_string(table)
            if table in tables:
                flag = True
            column = self.format_string(column)
            if table not in tables:
                flag = False
                sys.exit('No table as \'' + table + '\' exists')
            return table, column
        table_needed = ''
        f = 0
        for table in tables:
            if column in dict[table]:
                f += 1
                if f > 1:
                    flag = False
                    sys.exit('Invalid column name \'' + column + '\' given')
                table_needed = table
        if f == 0:
            flag = False
            sys.exit('No such column as \'' + column + '\' found')
        return table_needed, column


    def generate_eval(self, condition, table, table_info, data):
        condition = condition.split(' ')
        flag = True
        evaluator = ''
        for i in condition:
            i = self.format_string(i)
            if '.' in i:
                table_cur, column = self.search_col(i, [table], table_info)
                if table_cur != table:
                    flag = False
                    sys.exit('Table \'' + table_cur + '\' not found')
                elif column not in table_info[table]:
                    flag = False
                    sys.exit('No such column as \'' + column + '\' found in \'' + table_cur + '\' is given')
                evaluator += data[table_info[table_cur].index(column)]
            elif i == '=' and flag:
                evaluator += i*2
            elif i.lower() == 'and' or i.lower() == 'or':
                if flag:
                    pass
                evaluator += ' ' + i.lower() + ' '
            elif i in table_info[table]:
                if flag:
                    evaluator += data[table_info[table].index(i)]
            else:
                evaluator += i
        return evaluator


    def get_tables_col(self, columns, tables, table_info):
        cols_in_table = {}
        flag = True
        tables_needed = []
        if len(columns) == 1 and columns[0] == '*':
            if flag:
                flag = 1-flag
            for table in tables:
                cols_in_table[table] = []
                for column in table_info[table]:
                    flag = 1
                    cols_in_table[table].append(column)
            return cols_in_table, tables
        for column in columns:
            table, column = self.search_col(column, tables, table_info)
            flag = 1
            if table not in cols_in_table.keys():
                cols_in_table[table] = []
                flag = 0
                tables_needed.append(table)
            cols_in_table[table].append(column)
            if flag:
                pass
        return cols_in_table, tables_needed

    def join_needed_data(self, operator, tables, reqd_data, tables_data):
        #Joins the data needed for where clause
        operator_used = ""
        if operator == 'and':
            operator_used = "+"
            return self.join_data_and(tables, reqd_data)
        elif operator == 'or':
            operator_used = "|"
            return self.join_data_or(tables, reqd_data, tables_data)
        else:
            operator_used = "NULL"
            return self.join_data_single(tables, reqd_data, tables_data)
        #print operator_used


    def join_data_and(self, tables, reqd_data):
        #Join of data if the operator is AND
        flag = True
        data_final = []
        t0 = tables[0]
        table1 = self.format_string(t0)
        t1 = tables[1]
        table2 = self.format_string(t1)
        for i in reqd_data[table1]:
            for j in reqd_data[table2]:
                if flag:
                    data_final.append(i + j)

        return data_final


    def join_data_or(self, tables, reqd_data, tables_data):
        #Joins the data when the operator ins OR
        flag = True
        data_final = []
        t0 = tables[0]
        table1 = self.format_string(t0)
        t1 = tables[1]
        table2 = self.format_string(t1)
        for i in reqd_data[table1]:
            for j in tables_data[table2]:
                if j not in reqd_data[table2]:
                    flag = False
                    data_final.append(i + j)
        for i in reqd_data[table2]:
            for j in tables_data[table1]:
                if j not in reqd_data[table1]:
                    flag = False
                    data_final.append(j + i)

        return data_final

    def join_data_single(self, tables, reqd_data, tables_data):
        #Joins the data when there is no AND/OR operator
        flag = True
        data_final = []
        table1 = reqd_data.keys()[0]
        flg = False
        table2 = tables[1]
        if table1 == tables[1]:
            flag = False
            table2 = tables[0]
            flg = True
        for i in reqd_data[table1]:
            for j in tables_data[table2]:
                if not flg:
                    flag = True
                    data_final.append(j + i)
                else:
                    flag = False
                    data_final.append(i + j)

        return data_final


    def get_reqd_data(self, condition, tables, tables_data, table_info):
        operators = ['<', '>', '=']
        flag = True
        reqd_data = {}
        for i in condition:
            reqd = []
            for operator in operators:
                if operator in i:
                    reqd = i.split(operator)
                    break
            if len(reqd) != 2:
                flag = False
                sys.exit('Syntax ERROR: In where clause')
            r0 = reqd[0]
            table, column = self.search_col(self.format_string(r0), tables, table_info)
            reqd_data[table] = []
            i = i.replace(reqd[0], ' ' + column + ' ')
            for data in tables_data[table]:
                evaluator = self.generate_eval(i, table, table_info, data)
                try:
                    if eval(evaluator):
                        reqd_data[table].append(data)
                except NameError:
                    flag = False
                    sys.exit('AND cant be used in JOIN queries')
        return reqd_data


    def output(self, tables_needed, cols_in_table, table_info, tables_data, join):
        flag = True
        if join:
            table1 = tables_needed[0]
            header1 = self.print_header(table1, cols_in_table[table1])
            table2 = tables_needed[1]
            header2 = self.print_header(table2, cols_in_table[table2])
            print header1 + ',' + header2
            for i in tables_data:
                ans = ''
                for col in cols_in_table[table1]:
                    string_to_add = table_info[table1].index(col)
                    ans += i[string_to_add] + ','
                for col in cols_in_table[table2]:
                    string_to_add = table_info[table2].index(col) + len(table_info[table1])
                    ans += i[string_to_add] + ','
                print ans.strip(',')
        else:
            flag = False
            for table in tables_needed:
                print self.print_header(table, cols_in_table[table])
                for data in tables_data[table]:
                    ans = ''
                    for col in cols_in_table[table]:
                        string_to_add = table_info[table].index(col)
                        ans += data[string_to_add] + ','
                    print ans.strip(',') + ' '
                print
