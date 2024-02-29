import sys
import json
import winrm
import traceback
sys.path.insert(1, '/scripts')
from automation_utils import AutomationUtils

ticket_number = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
host = sys.argv[4]
shrinkable_log_files = sys.argv[5]
print(shrinkable_log_files)

utils = AutomationUtils()
ticket = utils.get_ticket(ticket_number)
configuration = utils.load_configuration()

jump_server = configuration['servers']['jump_server']
shrink_by_percentage = configuration['counts']['shrink_by_percentage']
winrm_session = winrm.Session(jump_server, auth=(username, password), transport='credssp', server_cert_validation='ignore')


def shrink_files():
    global shrinkable_log_files
    shrinkable_log_files = json.loads(shrinkable_log_files)

    if '.corp.riotinto.org' in host:
        db_instance = host
    else:
        db_instance = f'{host}.corp.riotinto.org'

    for log_file in shrinkable_log_files:
        shrink_by = (float(log_file['free_space_mb'])) * (float(shrink_by_percentage)/100)
        size_after_shrink = round(float(log_file['free_space_mb']) - shrink_by)
    
        ps_script = f'''
$db_instance = "{db_instance}"

$sql = "USE [{log_file['DBName']}]
GO
DBCC SHRINKFILE (N'{log_file['name']}', {size_after_shrink})
GO"

Invoke-Sqlcmd -ServerInstance $db_instance -Query $sql
'''
        print('Shrinking selected log files ....')
        ps_response = winrm_session.run_ps(ps_script)
        
        if ps_response.status_code == 0:
            print(f'{log_file["name"]} file for db {log_file["DBName"]} shrinked successfully')
            print(ps_response.std_out)

        else:
            print(f'Could not shrink {log_file["name"]} file for db {log_file["DBName"]}')
            print(ps_response.std_err)


if __name__ == '__main__':
    try:
        shrink_files()
    except:
        utils.error_occured(ticket, traceback.format_exc())
