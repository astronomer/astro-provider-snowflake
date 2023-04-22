# try:
#     from astro.sql.table import Table 
# except: 
from astronomer.providers.snowflake import Table
# from astronomer.providers.snowflake.decorators.snowpark import (
#     snowpark_python_task,
#     snowpark_virtualenv_task,
#     snowpark_ext_python_task
# )
# _SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

def test_task(df1:Table, df2:Table, str1:str, df6:Table, mydict, df3:Table, df4:Table):
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