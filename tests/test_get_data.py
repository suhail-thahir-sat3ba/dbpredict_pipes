import dbpredict_pipes as pipes
import pytest
from freezegun import freeze_time

##tests for ccs_icd9##
@pytest.fixture()
def ccs_icd9():
    ccs_icd9 = ['7955', '79551','79552','7956']
    return ccs_icd9


##tests for ccs_icd10##
@pytest.fixture()
def icd_clean(ccs_icd9):
    output = ['F529', 'N369', 'N3941', 'N3942', 'N3943', 'N3944', 'N3945']
    output_dict = {'icd9': ccs_icd9,
                   'icd10': output}
    return output_dict


@pytest.fixture()
def cpt_clean():
    output = ['0101T', '20974', '20979', '20980', '20981', '20982', '20983']
    out_dict = {'exclude' : False,
                'cpt_codes': output}
    return out_dict

@pytest.fixture()
def cpt_clean_excl():
    output = ['0101T', '20974', '20979', '20980', '20981', '20982', '20983']
    out_dict = {'exclude' : True,
                'cpt_codes': output}
    return out_dict

@pytest.fixture()
def dem_clean():
    dem_clean = {'dem_columns' : ['enrolledmons', 'pcpmons', 'rxmons', 
                                      'male', 'age']}
    return dem_clean


@pytest.fixture()
def phys_clean():
    phys_clean = {'phys_codes': ['10', '10', '13']}
    return phys_clean

@pytest.fixture()
def rx_clean():
    rx_clean = {'rx_cls': ['214028', '214028'],
                'rx_ther': ['13', '13', '14']}
    return rx_clean

@pytest.fixture()
def lab_clean():
    lab_clean = {'loinc_codes': ['10501-5', '10834-0', '10839-9', 
                                     '13457-7', 'PiperTazobact']}
    return lab_clean

@freeze_time("2020-01-01")   
def test_sql_inputs_demographics(dem_clean):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'end_date': '31-DEC-19',
             'dem_cols': 'enrolledmons, pcpmons, rxmons, male, age',
             'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('demographics', dem_clean,
                                         enrollee_qry)
    assert test==answer
    
@freeze_time("2020-01-01")   
def test_sql_inputs_rx(rx_clean):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'cls_codes': "'214028', '214028'",
             'ther_codes': "'13', '13', '14'",
             'start_date': '01-JAN-2015',
             'end_date': '31-DEC-2019',
              'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('drugs', rx_clean, enrollee_qry)
    assert test==answer

@freeze_time("2020-01-01")   
def test_sql_inputs_physicians(phys_clean):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'power_specs': "'OB/GYN', 'PATHOLOGY'",
             'health_rules_specs': "'Obstetrics & Gynecology', 'Pathology, Anatomic Pathology & Clinical Pathology', 'Pathology, Anatomic Pathology', 'Speech-Language Pathologist', 'Allergy & Immunology', 'Obstetrics & Gynecology, Gynecology', 'Pathology, Clinical Pathology/Laboratory Medicine', 'Pathology, Cytopathology', 'Obstetrics & Gynecology, Maternal & Fetal Medicine', 'Pathology, Dermatopathology', 'Obstetrics & Gynecology, Reproductive Endocrinology', 'Obstetrics & Gynecology, Gynecologic Oncology', 'Pathology, Clinical Pathology', 'Allergy & Immunology, Allergy', 'Pathology, Hematology', 'Obstetrics & Gynecology, Obstetrics', 'Pathology, Blood Banking & Transfusion Medicine', 'Obstetrics & Gynecology, Female Pelvic Medicine and Reconstructive Surgery', 'Pathology, Neuropathology', 'Speech, Language and Hearing Specialist/Technologist', 'Pathology, Molecular Genetic Pathology', 'Pathology, Pediatric Pathology', 'Pathology, Chemical Pathology', 'Allergy & Immunology, Clinical & Laboratory Immunology', 'Pathology, Clinical Laboratory Director, Non-physician', 'Obstetrics & Gynecology, Obesity Medicine', 'Obstetrics & Gynecology, Critical Care Medicine', 'Pathology, Immunopathology', 'Obstetrics & Gynecology, Hospice and Palliative Medicine', 'Pathology, Forensic Pathology', 'Pathology, Medical Microbiology'",
              'start_date': '01-JAN-2015',
              'end_date': '31-DEC-2019',
              'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('specialties', phys_clean, 
                                         enrollee_qry)
    assert test==answer
    
@freeze_time("2020-01-01")   
def test_sql_inputs_dx(icd_clean):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'start_date': '01-JAN-2015',
     'end_date': '31-DEC-2019',
     'dx_icd9': "'7955', '79551', '79552', '7956'",
     'dx_icd10': "'F529', 'N369', 'N3941', 'N3942', 'N3943', 'N3944', 'N3945'",
     'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('diagnoses', icd_clean, enrollee_qry)
    assert test==answer

@freeze_time("2020-01-01")   
def test_sql_inputs_labs(lab_clean):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'start_date': '01-JAN-2015',
     'end_date': '31-DEC-2019',
     'loincs': "'10501-5', '10834-0', '10839-9', '13457-7', 'PiperTazobact'",
     'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('labs', lab_clean, enrollee_qry)
    assert test==answer

@freeze_time("2020-01-01")   
def test_sql_inputs_cpt(cpt_clean):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'start_date': '01-JAN-2015',
     'end_date': '31-DEC-2019',
     'cpt_codes': "IN ('0101T', '20974', '20979', '20980', '20981', '20982', '20983')",
     'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('procedures', cpt_clean, enrollee_qry)
    assert test==answer
    
@freeze_time("2020-01-01")   
def test_sql_inputs_cpt_excl(cpt_clean_excl):
    enrollee_qry = pipes.get_data.get_enrollee_query('np_dc')
    
    answer = {'start_date': '01-JAN-2015',
 'end_date': '31-DEC-2019',
 'cpt_codes': "NOT IN ('0101T', '20974', '20979', '20980', '20981', '20982', '20983')",
     'enrollee_qry': enrollee_qry}
    
    test = pipes.get_data.get_sql_inputs('procedures', cpt_clean_excl, enrollee_qry)
    assert test==answer