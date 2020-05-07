from ._globals import __queries__ as query_path
import datetime
from dateutil.relativedelta import relativedelta

def get_data(data_type,model,criteria={}):
    '''
    DESCRIPTION

    Parameters
    ----------
    data_type : str
        String specifying which data type to pull. Valid options are 
        'enrollees', 'diagnoses', 'procedures', 'specialties', 'labs', 'drugs',
        and 'demographics'.
    model : str
        String specifying for which model data is pulled (i.e. for which 
        enrollees to make predictions.)
    criteria : dict, optional
        List of data elements to query (e.g. which diagnoses codes). 
        Default is {}.
    
    Returns
    -------
    path : str
        Path for parquet file containing pulled data.

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
    sql_inputs = get_sql_inputs(data_type,criteria,enrollee_qry)
    qry = get_sql_query(data_type,sql_inputs)
    
    # execute query
    # save parquet
    # return path
    
    path = ''
    return path
    

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
        # get_enrollee_qry('np_dc')
        pass                    # SET model_path HERE
    
    with open(model_path) as txt:
        qry = txt.read()
    
    qry = qry.format(**modeldict)
    
    return qry


def get_sql_inputs(data_type,criteria,enrollee_qry):
    '''
    DESCRIPTION

    Parameters
    ----------
    data_type : TYPE
        DESCRIPTION.
    criteria : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    
    now = datetime.datetime.now()
        
    t_end = now.replace(day=1)-relativedelta(days=1)
    t_start = t_end - relativedelta(years=5) + relativedelta(days=1)
        
        
    if data_type=='enrollees':
        pass


    elif data_type=='diagnoses':
        if 'icd9' not in criteria:
            raise KeyError("Expected 'icd9' in criteria.")
        if 'icd10' not in criteria:
            raise KeyError("Expected 'icd10' in criteria.")
        
        icd9 = criteria['icd9']
        icd10 = criteria['icd10']
        
        icd9str = str(icd9)[1:-1]
        icd10str = str(icd10)[1:-1]
        
        sql_inputs = {'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper(),
                      'dx_icd9' : icd9str,
                      'dx_icd10' : icd10str}
    
    
    elif data_type=='procedures':
        if 'exclude' not in criteria:
            raise KeyError("Expected 'exclude' in criteria.")
        if 'cpt_codes' not in criteria:
            raise KeyError("Expected 'cpt_codes' in criteria.")
        
        ex_flag = criteria['exclude']
        cpts = criteria['cpt_codes']
        
        if ex_flag:
            cpt_codes = 'NOT IN ('
        else:
            cpt_codes = 'IN ('
        
        cpt_codes += str(cpts)[1:-1] + ")"
        
        sql_inputs = {'start_date' : t_start.strftime("%d-%b-%Y").upper(),
                      'end_date' : t_end.strftime("%d-%b-%Y").upper(),
                      'cpt_codes' : cpt_codes}        
        
    
    
    elif data_type=='specialties':
        pass
    elif data_type=='labs':
        pass
    
    
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
        pass
    
    
    sql_inputs.update({'enrollee_qry' : enrollee_qry})
    
    return sql_inputs
    

def get_sql_query(data_type,sql_inputs):
    '''
    Creates the final SQL query based on frame and inputs.

    Parameters
    ----------
    data_type : string
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
                     'procedures' : 'proc_frame.txt',
                     'diagnoses' : 'dx_frame.txt'}
    
    with open(str(query_path) + "/" + filename_dict[data_type]) as txt:
        qry = txt.read()
    qry = qry.format(**sql_inputs)
        
    return qry
    
    
    
    
    
    
    
