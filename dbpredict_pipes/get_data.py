from ._globals import __queries__ as query_path
import datetime
from dateutil.relativedelta import relativedelta

def get_data(data_type,model,criteria=[]):
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
    criteria : list, optional
        List of data elements to query (e.g. which diagnoses codes). 
        Default is [].
    
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

    if type(criteria) is not list:
        raise TypeError("""criteria is incorrect type. Expected list but 
                        received {}.""".format(type(criteria)))
    
    if type(model) is not str:
        raise TypeError("""model is incorrect type. Expected str but received
                        {}""".format(type(model)))
    
    models_list = ['np_dc','d_c']
    if model not in models_list:
        raise ValueError ('{} is not a vaild model.'.format(model))
    
    
    # get SQL query inputs
    if criteria:
        sql_inputs = get_sql_inputs(data_type,criteria)
    else:
        sql_inputs = ''
    
    # get enrollees
    enrollee_qry = get_enrollee_query(model)
    
    # create query
    if data_type == 'enrollees':
        qry = enrollee_qry
    else:
        qry = get_sql_query(data_type,sql_inputs,enrollee_qry)
    
    # execute query
            ### SUHAIL ADD ###
    
    # save parquet
            ### SUHAIL ADD ###
    
    # return path
            ### SUHAIL ADD ###
    path = ''
    return path
    

def get_sql_inputs(data_type,criteria):
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
    
    if data_type=='enrollees':
        pass
    elif data_type=='diagnoses':
        pass
    elif data_type=='procedures':
        pass
    elif data_type=='specialties':
        pass
    elif data_type=='labs':
        pass
    elif data_type=='drugs':
        pass
    elif data_type=='demographics':
        pass
    
    sql_inputs = ''
    return sql_inputs
    

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
        pass                    # SET model_path HERE
    
    with open(model_path) as txt:
        qry = txt.read()
    
    qry = qry.format(modeldict)
    
    return qry


def get_sql_query(data_type,sql_inputs,enrollee_qry):
    '''
    DESCRIPTION

    Parameters
    ----------
    data_type : string
        DESCRIPTION.
    sql_inputs : string
        DESCRIPTION.
    enrollee_qry : string
        DESCRIPTION.

    Returns
    -------
    qry : string
        DESCRIPTION.

    '''
    
    if type(sql_inputs) is not str:
        raise TypeError("""sql_inputs is incorrect type. Expected str but 
                        received {}""".format(type(sql_inputs)))
    
    if type(enrollee_qry) is not str:
        raise TypeError("""enrollee_qry is incorrect type. Expected str but 
                        received {}""".format(type(enrollee_qry)))
    
    
    
    
    
    
    
    
    