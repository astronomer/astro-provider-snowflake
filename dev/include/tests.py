# try:
#     from astro.sql.table import Table 
# except: 
from astronomer.providers.snowflake import SnowparkTable
# from astronomer.providers.snowflake.decorators.snowpark import (
#     snowpark_python_task,
#     snowpark_virtualenv_task,
#     snowpark_ext_python_task
# )
# _SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

def test_task(df1:SnowparkTable, df2:SnowparkTable, str1:str, df6:SnowparkTable, mydict, df3:SnowparkTable, df4:SnowparkTable):
    from snowflake.snowpark import version as v
    from snowflake.snowpark.functions import col, sproc, udf
    
    snowpark_session.get_fully_qualified_current_schema()
    
    df1.show()
    df2.show()
    df3.show()
    df4.show()
    df6.show()
    mydict['mystr'] = str1

    return mydict['mystr']