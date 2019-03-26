import sys
import re
import csv
from other_func import *

oth = other_func()
FUNCS = ['distinct', 'sum', 'max', 'avg', 'min']

class definition_process:
    def __init__(self):
        self.comm_list = []
        self.clauses = []
        self.conditions = []
        self.tables = []
        self.tables_data = {}
        self.required_attr = []
        self.columns = []
        self.distinct_process = []

    def process_select(self):
        column_name = ''
        for i in self.required_attr:
            flg = False
            not_flg = True
            i = oth.format_string(i)
            for fn in FUNCS:
                if fn + '(' in i.lower():
                    flg = True
                    if ')' not in i:
                        not_flg = False
                        sys.exit('Syntax ERROR: \')\' missing')
                    else:
                        not_flg = False
                        column_name = i.strip(')').split(fn + '(')[1]
                    if fn == 'distinct':
                        not_flg = False
                        self.distinct_process.append(column_name)
                    else:
                        not_flg = False
                        self.fn_process.append([fn, column_name])
                    break
            if not flg:
                i = oth.format_string(i)
                not_flg = True
                if i != '':
                    self.columns.append(i.strip('()'))

class process(definition_process):

    def __init__(self, dict):
        self.dict = dict
        definition_process.__init__(self)
        self.fn_process = []

    def process_query(self,query):
        flg = True
        query = oth.format_string(query)
        if "from" not in query:
            flg = False
            sys.exit("Syntax ERROR: from statement missing")
        else:
            temp = query.split('from')
            before_time = str(temp[0])
            before_from = oth.format_string(before_time)
            after_time = str(temp[1])
            after_from = oth.format_string(after_time)

        if len(temp) != 2:
            flg = False
            sys.exit("Syntax ERROR: more than one from statement found")
        if 'select' not in before_from.lower():
            flg = False
            sys.exit("Syntax ERROR: no select statement found")
        elif query.lower().count('select') > 1:
            flg = False
            sys.exit("Syntax ERROR: more than one select statement found")

        where_string = 'where'
        self.clauses = after_from.split(where_string)
        clause_string = self.clauses[0]
        self.tables = oth.format_string(clause_string)
        delimiter_where = ','
        self.tables = self.tables.split(delimiter_where)

        for i in range(0, len(self.tables)):
            self.tables[i] = oth.format_string(self.tables[i])
            string_error = 'No such table as \'' + self.tables[i] + '\' Exists'
            if self.tables[i] not in self.dict.keys():
                flag = False
                sys.exit(string_error)
            self.tables_data[self.tables[i]] = oth.read_table_data(self.tables[i])

        self.required_attr = oth.format_string(before_from[len('select '):]).split(',')
        self.columns = []
        self.process_select()

        if len(self.columns) + len(self.distinct_process) + len(self.fn_process) < 1:
            flag = False
            sys.exit('Nothing is given to select')
        flag_passed = False 
        if len(self.distinct_process) != 0 and len(self.fn_process) != 0:
            flag = False
            sys.exit('Distinct and aggregate fns cant be given at the same time')
        if flag_passed != True:
            flag_passed = True
        if len(self.clauses) == 1 and len(self.tables) == 1 and len(self.fn_process) == 0 and len(self.distinct_process) == 0:
            if flag_passed:
                self.process_select_star()
        if len(self.distinct_process) == 1 and len(self.tables) == 1:
            if flag_passed:
                self.process_distinct()
        if len(self.distinct_process) > 1 and len(self.tables) == 1:
            if flag_passed:
                self.process_multiple_distinct()
        if len(self.clauses) > 1 and len(self.tables) == 1:
            if flag_passed:
                self.process_where()
        elif len(self.clauses) > 1 and len(self.tables) > 1:
            if flag_passed:
                self.process_multiple_where()
        elif len(self.tables) > 1:
            if flag_passed:
                self.process_join()
        elif len(self.fn_process) != 0:
            if flag_passed:
                self.process_agg()



    def process_select_star(self):
        flag = True
        if len(self.columns) == 1 and self.columns[0] == '*':
            self.columns = self.dict[self.tables[0]]
        print oth.print_header(self.tables[0], self.columns)
        for row in self.tables_data[self.tables[0]]:
            ans = ''
            for column in self.columns:
                fl = True
                indexed_column = self.dict[self.tables[0]].index(column)
                string_answer = row[indexed_column] + ','
                ans += string_answer
            print ans.strip(',')


    def process_where(self):
        self.clauses[1] = oth.format_string(self.clauses[1])
        columns = self.columns
        if len(self.columns) == 1 and self.columns[0] == '*':
            self.columns = self.dict[self.tables[0]]
        if len(columns) == 0:
            pass
        print oth.print_header(self.tables[0], self.columns)
        for row in self.tables_data[self.tables[0]]:
            ans = ''
            evaluator = oth.generate_eval(self.clauses[1], self.tables[0], self.dict, row)
            if eval(evaluator):
                for column in self.columns:
                    fl = True
                    indexed_column = self.dict[self.tables[0]].index(column)
                    string_answer = row[indexed_column] + ','
                    ans += string_answer
                print ans.strip(',')


    def process_multiple_where(self):
        condition = oth.format_string(self.clauses[1])
        phrase = condition
        flag = True
        logical_operators = ['<', '>', '=']
        operator = ''
        if 'OR' in condition:
            split_var = "OR"
            condition = condition.split(split_var)
            operator = split_var
        elif 'AND' in condition:
            split_var = "AND"
            condition = condition.split(split_var)
            operator = split_var
        else:
            condition = [condition]
            flag = False
        if len(condition) > 2:
            flag = False
            sys.exit('At max only one AND clause can be given')

        condition1 = condition[0]

        for i in logical_operators:
            if i in condition[0]:
                pass
            if i in condition1:
                temp = condition1.split(i)
                condition1 = temp
        if len(condition1) == 2 and '.' in condition1[1]:
            if operator in logical_operators:
                flag = True 
            self.process_where_join([condition, operator])
            return
        self.process_special_where(phrase)


    def process_where_join(self, clauses):
        reqd_data = {}
        flag = True
        fail_data = {}
        operators = ['<', '>', '=']
        for i in clauses[0]:
            operator = ''
            reqd = []
            i = oth.format_string(i)
            for op in operators:
                if op in i:
                    string_operator = ""
                    reqd = i.split(op)
                    string_operator = "null"
                    operator = op
                    if operator == '=':
                        string_operator = "equals to"
                        operator *= 2
                    break
            if len(reqd) > 2:
                flag = False
                sys.exit('Error occured in where condition')
            col_condn, table_condn = oth.get_tables_col(reqd, self.tables, self.dict)
            table1 = self.tables[0]
            col1 = self.dict[table1].index(col_condn[table1][0])
            reqd_data[i] = []
            table2 = self.tables[1]
            col2 = self.dict[table2].index(col_condn[table2][0])
            fail_data[i] = []
            for data in self.tables_data[table1]:
                for row in self.tables_data[table2]:
                    evaluator = data[col1] + operator + row[col2]
                    exp = eval(evaluator)
                    if exp:
                        s_dr = data + row
                        reqd_data[i].append(s_dr)
                    else:
                        s_dr = data + row
                        fail_data[i].append(s_dr)

        if clauses[1] != '':
            c1 = clauses[1]
            c0 = clauses[0]
            join_data = oth.join_needed_data(c1, c0, reqd_data, fail_data)
        else:
            join_data = []
            for i in reqd_data.keys():
                if len(join_data) == 0:
                    pass
                for j in reqd_data[i]:
                    join_data.append(j)
        self.columns, self.tables = oth.get_tables_col(self.columns, self.tables, self.dict)
        oth.output(self.tables, self.columns, self.dict, join_data, True)


    def process_special_where(self, sentence):
        flag = True
        condition = []
        operator = ''
        AND = 'and'
        if AND in sentence.lower().split():
            operator = AND
            condition = sentence.split(AND)
        elif 'or' in sentence.lower().split():
            OR = 'or'
            operator = OR
            condition = sentence.split(OR)
        else:
            condition = [sentence]
        #print condition
        reqd_data = oth.get_reqd_data(condition, self.tables, self.tables_data, self.dict)
        tables = self.tables
        cols_in_table, tables_needed = oth.get_tables_col(self.columns, self.tables, self.dict)
        tables_data = self.tables_data
        join_data = oth.join_needed_data(operator, tables_needed, reqd_data, self.tables_data)
        oth.output(tables_needed, cols_in_table, self.dict, join_data, flag) # FLAG should be true


    def process_distinct(self):
        data = []
        dp = self.distinct_process[0]
        table_needed, col = oth.search_col(self.distinct_process[0], self.tables, self.dict)
        print table_needed + '.' + col,
        print
        rem_data = []

        for i in self.tables_data[table_needed]:
            dp_0 = self.distinct_process[0]
            val = i[self.dict[table_needed].index(self.distinct_process[0])]
            temp_data = []
            if val not in data:
                data.append(val)
                print val,
                if val in data:
                    pass
                for j in self.columns:
                    temp_data.append(i[self.dict[table_needed].index(j)])
                    string_print = i[self.dict[table_needed].index(j)]
                    print string_print
                print
                rem_data.append(temp_data)


    def process_multiple_distinct(self):
        flag = True
        data = []
        for i in self.distinct_process:
            dp = self.distinct_process
            if i not in dp:
                pass
            table_needed, col = oth.search_col(i, self.tables, self.dict)
            tn = self.tables_data[table_needed]
            for j in self.tables_data[table_needed]:
                val = j[self.dict[table_needed].index(i)]
                if val not in data:
                    if j not in self.tables_data[table_needed]:
                        pass
                    data.append(val)
                    print val


    def process_agg(self):
        #for agg fns
        header = ''
        MIN = "min"
        result = ''
        MAX = "max"
        for query in self.fn_process:
            fn_name = query[0]
            flag = True
            column_name = query[1]
            table = ''
            col = ''
            if '.' in column_name:
                table, col = column_name.split('.')
            else:
                count = 0
                for i in self.tables:
                    if column_name in self.dict[i]:
                        table = i
                        col = column_name
                        count += 1
                if count == 0:
                    flag = False
                    sys.exit('No such column \'' + column_name + '\' found')
                elif count > 1:
                    flag = False
                    sys.exit('Ambiguous column name \'' + column_name + '\' given')
            data = []
            AVG = "avg"
            header += table + '.' + col + ','
            for row in self.tables_data[table]:
                data.append(int(row[self.dict[table].index(col)]))
            SUM = "sum"
            if fn_name.lower() == MIN:
                string_to_add = str(min(data))
                result += string_to_add
            elif fn_name.lower() == MAX:
                string_to_add = str(max(data))
                result += string_to_add
            elif fn_name.lower() == SUM:
                string_to_add = str(sum(data))
                result += string_to_add
            elif fn_name.lower() == "avg" or fn_name.lower() == "average":
                result += str(float(sum(data)) / len(data))
            result += ','
        header.strip(',')
        print header
        print result


    def process_join(self):
        cols_in_table, tables_needed = oth.get_tables_col(self.columns, self.tables, self.dict)
        join_data = []
        flag = True
        if len(tables_needed) == 2:
            tn = tables_needed
            t1 = tn[0]
            t2 = tn[1]
            for i in self.tables_data[t1]:
                if i in t2:
                    pass
                for j in self.tables_data[t2]:
                    join_data.append(i + j)
            oth.output(tables_needed, cols_in_table, self.dict, join_data, join = True)
        else:
            oth.output(tables_needed, cols_in_table, self.dict, join_data, join = False)
