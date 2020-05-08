import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import sqlalchemy as sqa
import cx_Oracle

def push_data(data, output_format, model, cred={}):
    '''Takes predictions and saves results
    Inputs
    -------
    data : pandas.core.frame.DataFrame with 'id' column and prediction
    output_format :  string of output choice
    model: string of model choice
    cred: dictionary containg keys 'username' and 'password'
    
    Returns
    -------
    Error or True
    '''
    
    now = datetime.datetime.now()

    prediction_windows={'np_dc': 12,
                        'd_c': 24}
    
    data = data.rename(columns={'id': 'empi'})
    data['run_date'] = now.date().strftime("%d-%b-%y").upper()
    data['pred_window_start'] = now.replace(day=1).strftime("%d-%b-%y").upper()
    data['pred_window_end'] = (now.replace(day=1) + relativedelta(
        months=prediction_windows[model])).strftime("%d-%b-%y").upper()
    
    if output_format == 'sql':
        outcome = push_sql(data, 'sample_table_name', cred)
    return outcome

def push_sql(data, sql_table, cred):
    '''pushes data to sql
    Inputs
    -------
    data : pandas.core.frame.DataFrame
    sql_table: str that will be the SQL table name
    cred: dictionary containg keys 'username' and 'password'
    
    Returns
    -------
    None
    '''
    
    oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +
    cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))

    engine = sqa.create_engine(
        oracle_connection_string.format(
            username=cred['username'],
            password=cred['password'],
            hostname='db_edwprd',
            port='1521',
            service_name='edwprd',
        )
    )
    try:
        data.to_sql(sql_table, engine)
    except Exception as e:
        return e
    else:
        return True

