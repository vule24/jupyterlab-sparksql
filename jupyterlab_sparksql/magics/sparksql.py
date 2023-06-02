from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from string import Formatter

from .widget import generate_output_widget


@magics_class
class SparkSQL(Magics):

    @property
    def spark(self):
        return SparkSession._instantiatedSession

    @magic_arguments()
    @argument('dataframe', metavar='DF', type=str, nargs='?')
    @argument('-n', '--num-rows', type=int, default=100)
    @cell_magic
    def sql(self, line, cell):
        self.create_temp_view_for_available_dataframe()
        
        args = parse_argstring(self.sql, line)
        query_str = rf'{self.format_fillin_pyvar(cell)}'
        
        sdf = self.spark.sql(query_str)
        if args.dataframe:
            self.shell.user_ns.update({args.dataframe: sdf})

        return generate_output_widget(sdf, num_rows=args.num_rows, export_table_name=args.dataframe or None)



    @magic_arguments()
    @argument('dataframe', metavar='DF', type=str, nargs='?')
    @argument('-n', '--num-rows', type=int, default=20)
    @line_magic
    def show(self, line):
        args = parse_argstring(self.sql, line)
        if not args.dataframe:
            raise ValueError('dataframe is required. Eg. `%show <dataframe>`')
        sdf = self.shell.user_ns.get(args.dataframe, None)
        if not sdf:
            raise NameError(f"Name '{args.dataframe}' is not defined")
        
        return generate_output_widget(sdf, num_rows=args.num_rows, export_table_name=args.dataframe)


    def create_temp_view_for_available_dataframe(self):
        for k, v in self.shell.user_ns.items():
            v.createOrReplaceTempView(k) if isinstance(v, DataFrame) else None

    def format_fillin_pyvar(self, source):
        params = [fn for _, fn, _, _ in Formatter().parse(source) if fn]
        params_values = {}
        for param in params:
            value = self.shell.user_ns.get(param, None)
            if not value:
                raise NameError("name '{}' is not defined".format(param))
            params_values.update({param: value})
        return source.format(**params_values)
    



