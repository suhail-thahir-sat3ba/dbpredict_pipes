import cx_Oracle
import sqlalchemy as sqa
import getpass

def get_creds():
    oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +
    cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
    
    connected = False
    while not connected:
        username = input("Enter SQL username: ")
        password = getpass.getpass("Password: ")
        cred = {'username':username,
                 'password':password}
    
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
            engine.connect()
            connected = True     
        except Exception as e:
            print(e)
    return cred
