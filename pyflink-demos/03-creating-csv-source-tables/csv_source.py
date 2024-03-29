from pyflink.table import (
    DataTypes, TableEnvironment, EnvironmentSettings, 
    CsvTableSource
)


def main():
    env_settings = EnvironmentSettings.new_instance()\
                        .in_batch_mode()\
                        .build()
    tbl_env = TableEnvironment.create(env_settings)

    field_names = ['seller_id', 'product', 'quantity', 'product_price', 'sales_date']
    field_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.DATE()]
    source = CsvTableSource(
        '/home/ec2-user/flink/code/pyflink-demos/03-creating-csv-source-tables/csv-input',
        field_names,
        field_types,
        ignore_first_line=True
    )
    tbl_env.register_table_source('product_locale_sales', source)

    tbl = tbl_env.from_path('product_locale_sales')

    print('\nProduct Sales Schema')
    tbl.print_schema()

    print('\nProduct Locale Sales Data')
    print(tbl.to_pandas())



if __name__ == '__main__':
    main()
