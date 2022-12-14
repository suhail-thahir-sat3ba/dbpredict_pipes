from ._globals import __queries__ as query_path, __xwalks__ as xwalk_path
from ._globals import __temp__ as temp_path
import datetime
from dateutil.relativedelta import relativedelta
import sqlalchemy as sqa
import pandas as pd
import cx_Oracle
import os

def get_data(data_type,model,cred, criteria={}):
    '''
    Retrieve data from SQL server for a particular model based on criteria.

    Parameters
    ----------
    data_type : str
        String specifying which data type to pull. Valid options are 
        'enrollees', 'diagnoses', 'procedures', 'specialties', 'labs', 'drugs',
        and 'demographics'.
    model : str
        String specifying for which model data is pulled (i.e. for which 
        enrollees to make predictions.)
    cred : dict
        SQL server login credentials. Dictionary with keys: 'username' and 
        'password'.
    criteria : dict, optional
        List of data elements to query (e.g. which diagnoses codes). 
        Default is {}.
    
    Returns
    -------
    data_path : str
        Path for HDF file containing pulled data.

    '''
    
    ## check inputs
    if type(data_type) is not str:
        raise TypeError("""data_type is incorrect type, Expect str but 
                        received {}""".format(type(data_type)))
    
    data_types = ['enrollees','diagnoses','procedures','specialties',
                  'labs','drugs','demographics']
    if data_type not in data_types:
        raise ValueError ('{} is not a valid data type.'.format(data_type))

    if type(criteria) is not dict:
        raise TypeError("""criteria is incorrect type. Expected dict but 
                        received {}.""".format(type(criteria)))
    
    if type(model) is not str:
        raise TypeError("""model is incorrect type. Expected str but received
                        {}""".format(type(model)))
    
    models_list = ['np_dc','d_c']
    if model not in models_list:
        raise ValueError ('{} is not a vaild model.'.format(model))
    
    
    enrollee_qry = get_enrollee_query(model)

    sql_inputs = get_sql_inputs(data_type,criteria,enrollee_qry, cred)
    qry = get_sql_query(data_type,sql_inputs)
    
    data_chunks =  execute_query(qry, cred, data_type)
    data_path = save_data(data_chunks, data_type)
            
    return data_path
    

def get_enrollee_query(model):
    '''
    Generates a SQL query for pulling correct enrollees for specified 
    prediction model.

    Parameters
    ----------
    model : str
        String specifying for which model data is pulled (i.e. for which 
        enrollees to make predictions.)

    Returns
    -------
    qry : str
        SQL query to pull enrollee IDs.

    '''
    
    if model == 'np_dc':
        now = datetime.datetime.now()
        
        t = [now - relativedelta(months=i) for i in range(1,7)]
               
        modeldict = {'t6m' : str(t[5].month).zfill(2), 't6y' : t[5].year,
                     't5m' : str(t[4].month).zfill(2), 't5y' : t[4].year,
                     't4m' : str(t[3].month).zfill(2), 't4y' : t[3].year,
                     't3m' : str(t[2].month).zfill(2), 't3y' : t[2].year,
                     't2m' : str(t[1].month).zfill(2), 't2y' : t[1].year,
                     't1m' : str(t[0].month).zfill(2), 't1y' : t[0].year,
                     'today' : now.strftime("%d-%b-%y").upper()}
        
        model_path = str(query_path) + '/np_dc.txt'
        
    elif model == 'd_c':
        now = datetime.datetime.now()
        t_end = now.replace(day=1)-relativedelta(days=1)        
        t_start = t_end - relativedelta(months=12)

        t = [now - relativedelta(months=i) for i in range(1,7)]
               
        modeldict = {'t6m' : str(t[5].month).zfill(2), 't6y' : t[5].year,
                     't5m' : str(t[4].month).zfill(2), 't5y' : t[4].year,
                     't4m' : str(t[3].month).zfill(2), 't4y' : t[3].year,
                     't3m' : str(t[2].month).zfill(2), 't3y' : t[2].year,
                     't2m' : str(t[1].month).zfill(2), 't2y' : t[1].year,
                     't1m' : str(t[0].month).zfill(2), 't1y' : t[0].year,
                     't_end' : t_end.strftime("%d-%b-%y").upper(),
                     't_start': t_start.strftime("%d-%b-%y").upper(),
                     'tend_yyyymm': str(t_end.year) +str(t_end.month).zfill(2),
                     'tstart_yyyymm':str(t_start.year) + str(t_start.month
                                                            ).zfill(2)
                     }
        
        model_path = str(query_path) + '/d_c.txt'
        
    
    with open(model_path) as txt:
        qry = txt.read()
    
    qry = qry.format(**modeldict)
    
    return qry

def get_sql_inputs(data_type,criteria,enrollee_qry, cred):
    '''
    Parses criteria to create snippets for SQL query.

    Parameters
    ----------
    data_type : str
        String specifying which data type to pull.
    criteria : dict
        List of data elements to query (e.g. which diagnoses codes).
    enrollee_qry : str
        Query snipet to select enrollees for model.
    cred : dict
        SQL server login credentials. Dictionary with keys: 'username' and 
        'password'.

    Returns
    -------
    sql_inputs : dict
        Dictionary with snippets for SQL query.

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
   
    now = datetime.datetime.now()
        
    t_end = now.replace(day=1)-relativedelta(days=1)
    t_start = t_end - relativedelta(years=5) + relativedelta(days=1)
        
    sql_inputs = {}    
    if data_type=='enrollees':
        pass

    elif data_type=='diagnoses':
        if 'icd9' not in criteria:
            raise KeyError("Expected 'icd9' in criteria.")
        if 'icd10' not in criteria:
            raise KeyError("Expected 'icd10' in criteria.")
        
        icd9 = criteria['icd9']
        icd10 = criteria['icd10']
        
        icd9_df = pd.DataFrame({'dx_code': icd9})
        icd9_df['diag_vrsn_ind'] = 'ICD-9'
        icd10_df = pd.DataFrame({'dx_code': icd10})
        icd10_df['diag_vrsn_ind'] = 'ICD-10'
        
        icd_df = icd9_df.append(icd10_df)
        icd_df.to_sql('icd_keep', engine, if_exists='replace',
                      dtype = {'dx_code': sqa.types.VARCHAR(length=20),
                               'diag_vrsn_ind': sqa.types.VARCHAR(length=20)})
        
        sql_inputs = {'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper()
                      }
    
    
    elif data_type=='procedures':
        if 'exclude' not in criteria:
            raise KeyError("Expected 'exclude' in criteria.")
        if 'cpt_codes' not in criteria:
            raise KeyError("Expected 'cpt_codes' in criteria.")
        
        ex_flag = criteria['exclude']
        cpts = criteria['cpt_codes']
        
        cpt_df = pd.DataFrame({'cpt_code': cpts})
        cpt_df.to_sql('cpt_keep', engine, if_exists='replace',
                      dtype={'cpt_code': sqa.types.VARCHAR(length=20)})
        
        if ex_flag:
            exclude = 'NOT '
        else:
            exclude = ''
        
        
        sql_inputs = {'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper(),
                      'exclude' : exclude}        
    
    
    elif data_type=='specialties':
        ama_keys = criteria['phys_codes']
        
        key_ama_path = str(xwalk_path) + '/AMA_spec.pickle' 
        key_to_ama = pd.read_pickle(key_ama_path)
        ama_specs = list(key_to_ama[key_to_ama['AMA_key'].isin(ama_keys)]['AMA_Equivalent'])
        
        ama_power_path = str(xwalk_path) + '/power_to_AMA.pickle'
        ama_to_power = pd.read_pickle(ama_power_path)
        power_specs = ama_to_power[ama_to_power['AMA_Equivalent'].isin(ama_specs)][['power_names']]
        power_specs.to_sql('power_spec_keep', engine, if_exists='replace',
                           dtype={'power_names': sqa.types.VARCHAR(length=100)}) 
        
        ama_hr_path = str(xwalk_path) + '/healthrules_to_AMA.pickle'
        ama_to_healthrules = pd.read_pickle(ama_hr_path)
        hr_specs = ama_to_healthrules[ama_to_healthrules['AMA_Equivalent'].isin(ama_specs)][['TXNMY_DESC']]
        hr_specs.to_sql('hr_spec_keep', engine, if_exists='replace',
                        dtype={'TXNMY_DESC':  sqa.types.VARCHAR(length=100)})
        
        
        sql_inputs = {'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper()}
        

    elif data_type=='labs':
        if 'loinc_codes' not in criteria:
            raise KeyError("Expected 'loinc_codes' in criteria.")
        
        loincs = criteria['loinc_codes']
        loincs_str = str(loincs)[1:-1]
        
        sql_inputs = {'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper(),
                      'loincs' : loincs_str}
    
    
    elif data_type=='drugs':
        if 'rx_cls' not in criteria:
            raise KeyError("Expected 'rx_cls' in criteria.")
        if 'rx_ther' not in criteria:
            raise KeyError("Expected 'rx_ther' in criteria.")
        
        rx_cls = criteria['rx_cls']
        rx_cls_str = str(rx_cls)[1:-1]
        
        rx_ther = criteria['rx_ther']
        rx_ther_str = str(rx_ther)[1:-1]
        
        sql_inputs = {'cls_codes' : rx_cls_str,
                      'ther_codes' : rx_ther_str,
                      'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper()}
    
    
    elif data_type=='demographics':
        if 'dem_columns' not in criteria:
            raise KeyError("Expected 'dem_columns' in criteria.")
        
        #post enrollees qry to sql
        try:
            engine.execute("drop table data_pull_members")
        except Exception as e:
            if type(e) != sqa.exc.DatabaseError:
                raise e
        engine.execute("create table data_pull_members as {} select * from data_pull_members".format(enrollee_qry))
        
        dem_cols = criteria['dem_columns']

        dem_cols_str = str(dem_cols)[1:-1].replace("'","")
        
        sql_inputs = {'end_date' : t_end.strftime("%d-%b-%y").upper(),
                      'dem_cols' : dem_cols_str}
    
    if data_type != "demographics":
        sql_inputs.update({'enrollee_qry' : enrollee_qry})
    
    return sql_inputs
    

def get_sql_query(data_type,sql_inputs):
    '''
    Creates the final SQL query based on frame and inputs.

    Parameters
    ----------
    data_type : str
        String specifying which data type to pull.
    sql_inputs : dict
        Dictionary of inputs to the SQL query frame.
    
    Returns
    -------
    qry : string
        Final SQL query.

    '''
    
    if type(sql_inputs) is not dict:
        raise TypeError("""sql_inputs is incorrect type. Expected dict but 
                        received {}""".format(type(sql_inputs)))
    
    filename_dict = {'enrollees' : 'enrollee_frame.txt',
                     'drugs': 'rx_frame.txt',
                     'specialties': 'phys_frame.txt',
                     'procedures' : 'proc_frame.txt',
                     'diagnoses' : 'dx_frame.txt',
                     'labs' : 'lab_frame.txt',
                     'demographics' : 'dem_frame.txt'}
    
    with open(str(query_path) + "/" + filename_dict[data_type]) as txt:
        qry = txt.read()
    qry = qry.format(**sql_inputs)
        
    return qry

def execute_query(qry, cred, data_type):
    '''
    Executes SQL query. Drops any intermediate tables created in get_data

    Parameters
    ----------
    qry : str
        SQL query to execute.
    cred : dict
        SQL server login credentials. Dictionary with keys: 'username' and 
        'password'.
    data_type: str
        data_type being pulled

    Returns
    -------
    df_chunks : iterator
        Iterable data chucks from query.

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

    df_chunks = pd.read_sql(qry, engine, chunksize=10000000)
    
    if data_type == "demographics":
        engine.execute("drop table data_pull_members")
    if data_type =="procedures":
        engine.execute("drop table cpt_keep")
    if data_type=="diagnoses":
        engine.execute("drop table icd_keep")
    if data_type=="specialties":
        engine.execute("drop table hr_spec_keep")
        engine.execute("drop table power_spec_keep")
    return df_chunks

def save_data(chunks, data_type):
    '''
    Saves HDF file of data returned from query.

    Parameters
    ----------
    chunks : iterator
        Iterable data chucks from query.
    data_type : str
        String specifying which data type to pull.

    Returns
    -------
    data_path : str
        Path to saved HDF data.

    '''
    data_path = str(temp_path) + "/{}.h5py".format(data_type)
        
    columns_names_map = rename_columns(data_type)

    if data_type == 'specialties':
        #retrieve ama and power specialties xwalk
        ama_power_path = str(xwalk_path) + '/power_to_AMA.pickle'
        ama_to_power = pd.read_pickle(ama_power_path)
        
        #retrieve ama and health rules specialties crosswalk
        ama_hr_path = str(xwalk_path) + '/healthrules_to_AMA.pickle'
        ama_to_healthrules = pd.read_pickle(ama_hr_path)
        
        #retrieve AMA to key xwalk
        key_ama_path = str(xwalk_path) + '/AMA_spec.pickle' 
        key_to_ama = pd.read_pickle(key_ama_path)
            
    try:
        n= 0
        for df in chunks:
            for col in df.columns:
                df[col] = df[col].astype('str')
            if data_type=='specialties':
                power_df = df[df['phys_type']=='pwr']
                power_df = power_df.merge(ama_to_power, how='left', left_on='specialty', right_on='power_names')
                power_df = power_df[['empi', 'AMA_Equivalent', 'serv_line_start_date']]
                
                hr_df = df[df['phys_type']=='hr']
                hr_df = hr_df.merge(ama_to_healthrules, how='left', left_on='specialty', right_on='TXNMY_DESC')
                hr_df = hr_df[['empi', 'AMA_Equivalent', 'serv_line_start_date']]
                
                df = hr_df.append(power_df)
                df =df.merge(key_to_ama, how='left', left_on='AMA_Equivalent', right_on='AMA_Equivalent')
                df = df[['empi', 'AMA_key', 'serv_line_start_date']]
                
            df = df.rename(columns=columns_names_map)
            if 'svcdate' in df.columns:
                df['svcdate'] = df['svcdate'].astype('<M8[ns]')
            if n == 0:
                df.to_hdf(data_path, mode ='w', format='table', key='mbr_id')
            else:
                df.to_hdf(data_path, mode ='a', format='table', append=True, 
                          key='mbr_id')
            n += 1
    except Exception as e:
        if os.path.exists(data_path):
            os.remove(data_path)
        print("Error in saving {}.h5py.".format(data_type))
        print("Error type: {}".format(type(e).__name__))
        print(e)
    else:
        return data_path

def rename_columns(data_type):
    '''return a dictionary mapping imported column names to dbpredict column 
    names
    '''
    demographics = {'empi': 'id',
                    'enrolledmons': 'DEM_enrolledmons',
                    'pcpmons': 'DEM_pcpmons',
                    'rxmons': 'DEM_rxmons',
                    'pcpind': 'DEM_pcpind',
                    'rxind': 'DEM_rxind',
                    'male': 'DEM_male',
                    'age': 'DEM_age',
                    'lobcom': 'DEM_lobcom',
                    'lobmcr': 'DEM_lobmcr',
                    'lobmcd': 'DEM_lobmcd'
                     }
    
    diagnoses = {'empi': 'id',
                 'dx_code': 'code',
                 'serv_line_start_date': 'svcdate',
                 'diag_vrsn_ind' : 'category'}
    
    procedures = {'empi': 'id',
                  'cpt_code': 'code',
                  'serv_line_start_date': 'svcdate'} 
    
    specialties = {'empi': 'id',
                   'AMA_key': 'code',
                   'serv_line_start_date': 'svcdate'}
    
    labs = {'empi': 'id',
            'loinc_code': 'code',
            'rslt': 'category',
            'serv_date': 'svcdate'} 
    
    drugs = {'empi': 'id',
             'gpi_class': 'code',
             'serv_date': 'svcdate',
             'type' : 'category'
             }
    
    enrollees ={'empi': 'id'}
    
    column_dict= {'demographics': demographics,
                  'diagnoses' : diagnoses,
                  'procedures': procedures,
                  'specialties': specialties,
                  'labs': labs,
                  'drugs': drugs,
                  'enrollees': enrollees}
    
    
    return column_dict[data_type]
