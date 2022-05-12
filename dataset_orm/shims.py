from dataset.table import Table, and_, false


def _args_to_clause(self, args, clauses=()):
    # drop-in replacement for dataset.Table._args_to_clause
    # to support col__op==value syntax on filters
    # TODO remove pending https://github.com/pudo/dataset/pull/396
    clauses = list(clauses)
    for column, value in args.items():
        column = self._get_column_name(column)
        if '__' in column:
            column, op = column.split('__', 1)
            clauses.append(self._generate_clause(column, op, value))
        elif not self.has_column(column):
            clauses.append(false())
        elif isinstance(value, (list, tuple, set)):
            clauses.append(self._generate_clause(column, "in", value))
        elif isinstance(value, dict):
            for op, op_value in value.items():
                clauses.append(self._generate_clause(column, op, op_value))
        else:
            clauses.append(self._generate_clause(column, "=", value))
    return and_(True, *clauses)


Table._args_to_clause = _args_to_clause
