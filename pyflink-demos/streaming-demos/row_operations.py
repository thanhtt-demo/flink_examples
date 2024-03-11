from statistics import stdev, mean

from pyflink.common import Row
from pyflink.table import (
    DataTypes,
    TableEnvironment,
    EnvironmentSettings, 
    CsvTableSource
)
from pyflink.table.udf import udf


@udf(result_type=DataTypes.ROW([DataTypes.FIELD('seller_id', DataTypes.STRING()),
                                DataTypes.FIELD('sales_total', DataTypes.INT()),
                                DataTypes.FIELD('qtr_avg', DataTypes.DOUBLE()),
                                DataTypes.FIELD('qtr_stdev', DataTypes.DOUBLE())]))
def sales_summary_stats(seller_sales: Row) -> Row:
    seller_id, q1, q2, q3, q4 = seller_sales
    sales = (q1, q2, q3, q4)
    total_sales = sum(sales)
    qtr_avg = round(mean(sales), 2)
    qtr_stdev = round(stdev(sales), 2)
    return Row(seller_id, total_sales, qtr_avg, qtr_stdev)


def main():
    env_settings = EnvironmentSettings.new_instance()\
                        .in_batch_mode()\
                        .build()
    tbl_env = TableEnvironment.create(env_settings)

    field_names = ['seller_id', 'q1', 'q2', 'q3', 'q4']
    field_types = [DataTypes.STRING(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT()]
    source = CsvTableSource(
        '/home/ec2-user/flink/code/pyflink-demos/streaming-demos/csv-input/',
        field_names,
        field_types,
        ignore_first_line=True
    )
    tbl_env.register_table_source('quarterly_sales', source)

    tbl = tbl_env.from_path('quarterly_sales')

    print('\nQuarterly Sales Schema')
    tbl.print_schema()

    print('\nQuarterly Sales Data')
    print(tbl.to_pandas().sort_values('seller_id'))

    sales_stats = tbl.map(sales_summary_stats).alias('seller_id',
                                                    'total_sales',
                                                    'quarter_avg',
                                                    'quarter_stdev')

    print('\nSales Summary Stats schema')
    sales_stats.print_schema()

    print('\nSales Summary Stats data')
    print(sales_stats.to_pandas().sort_values('seller_id'))


if __name__ == '__main__':
    main()
