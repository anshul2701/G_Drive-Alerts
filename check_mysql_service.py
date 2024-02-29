import sys
import winrm
import traceback
sys.path.insert(1, '/scripts')
from automation_utils import AutomationUtils

ticket_number = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
host = sys.argv[4]

utils = AutomationUtils()
ticket = utils.get_ticket(ticket_number)
configuration = utils.load_configuration()

jump_server = configuration['servers']['jump_server']
winrm_session = winrm.Session(jump_server, auth=(username, password), transport='credssp', server_cert_validation='ignore')


def check_ms_sql_servcies():
    if '.corp.riotinto.org' in host:
        db_instance = host
    else:
        db_instance = f'{host}.corp.riotinto.org'
    
    ps_script = '''
$db_instance= "''' + db_instance + '''"
Get-service -ComputerName $db_instance  | where {($_.name -like "MSSQL$*" -or $_.name -like "MSSQLSERVER" -or $_.name -like "SQL Server (*")} | Format-List
'''

    print(f'Checking MSSQL service status for {db_instance} ....')
    for i in range(3):                      ## adding retries for mssql service checks as it fails sometimes (during testing) in first attempt
        ps_response = winrm_session.run_ps(ps_script)

        if ps_response.status_code == 0:
            print(f'MSSQL service status fetched successfully from {db_instance}')
            print(ps_response.std_out)
            service_status = utils.format_ps_output(ticket, ps_response.std_out.decode())
            
            branch = 0
            
            if (len(service_status) >= 1) and (service_status[0]['Status'] == 'Running'):
                branch = 1
    
            else:
                branch = 2
                
                worknote = f'No active MSSQL service found for {db_instance}'
                print(f'#JOB_RESULT worknote = {worknote}')
    
            print(f'#JOB_RESULT branch = {branch}')
            break
    
        else:
            if i < 2:
                continue
            print(f'Error in fetching MSSQL service status details from {db_instance}')
            print(ps_response.std_err)
    
            utils.error_occured(ticket, 'error')
    

if __name__ == '__main__':
    try:
        check_ms_sql_servcies()
    except:
        utils.error_occured(ticket, traceback.format_exc())
