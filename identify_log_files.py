import re
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

utils = AutomationUtils()
ticket = utils.get_ticket(ticket_number)
configuration = utils.load_configuration()

jump_server = configuration['servers']['jump_server']
max_log_files_count = configuration['counts']['max_log_files_count']
winrm_session = winrm.Session(jump_server, auth=(username, password), transport='credssp', server_cert_validation='ignore')


def get_db_files():
    if '.corp.riotinto.org' in host:
        db_instance = host
    else:
        db_instance = f'{host}.corp.riotinto.org'

    ps_script = f'''
$db_instance = "{db_instance}"

$sql = "CREATE TABLE #filespaceinfo
(DBName NVARCHAR(100),fileid INT,groupid int, file_size_mb FLOAT, space_used_mb FLOAT, free_space_mb FLOAT, name NVARCHAR(100),
filename NVARCHAR(1000))
insert into #filespaceinfo
exec sp_MSforeachdb 'use [?];
select db_name() as DBName,
a.fileid,a.groupid,
[file_size_mb] =
convert(decimal(12,2),round(a.size/128.000,2)),
[space_used_mb] =
convert(decimal(12,2),round(fileproperty(a.name,''spaceused'')/128.000,2)),
[free_space_mb] =
convert(decimal(12,2),round((a.size-fileproperty(a.name,''spaceused''))/128.000,2)) ,
name = a.name,
filename = a.filename
from
dbo.sysfiles a '

select * from #filespaceinfo where DBName not in ('master','model','msdb','tempdb') order by free_space_mb desc
drop table #filespaceinfo"

Invoke-Sqlcmd -ServerInstance $db_instance -Query $sql | Format-List
'''

    print(f'Fetching MSSQL file details for {db_instance} ....')
    ps_response = winrm_session.run_ps(ps_script)
    
    if ps_response.status_code == 0:
        print(f'MSSQL file details fetched successfully from {db_instance}')
        print(ps_response.std_out)

        worknote = f'Before shrinking MSSQL file status:\n\n{ps_response.std_out.decode()}'
        utils.update_worknote(ticket, worknote)

        db_files = utils.format_ps_output(ticket, ps_response.std_out.decode())
        return db_files

    else:
        print(f'Error in fetching DB file details from {db_instance}')
        print(ps_response.std_err)
        utils.error_occured(ticket, 'error')


def get_transaction_status():
    if '.corp.riotinto.org' in host:
        db_instance = host
    else:
        db_instance = f'{host}.corp.riotinto.org'

    ps_script = f'''
$db_instance = "{db_instance}"

$sql = "select name, log_reuse_Wait_desc from sys.databases"

Invoke-Sqlcmd -ServerInstance $db_instance -Query $sql | Format-List
'''

    print(f'Fetching MSSQL log reuse status for {db_instance} ....')
    ps_response = winrm_session.run_ps(ps_script)

    if ps_response.status_code == 0:
        print(f'Log reuse details fetched successfully from {db_instance}')
        print(ps_response.std_out)

        worknote = f'Before shrinking MSSQL Log Reuse status:\n\n{ps_response.std_out.decode()}'
        utils.update_worknote(ticket, worknote)
        
        transaction_status = utils.format_ps_output(ticket, ps_response.std_out.decode())
        return transaction_status

    else:
        print(f'Error in fetching log reuse status details from {db_instance}')
        print(ps_response.std_err)
        utils.error_occured(ticket, 'error')


def get_shrinkable_log_files(db_files, transaction_status):
    log_reuse_status = {i['name']:i['log_reuse_Wait_desc'] for i in transaction_status}
    shrinkable_log_files = []
    
    regex = r'(G:\\).+(\.ldf)'
    
    for db_file in db_files:
        if re.search(regex, db_file['filename']) and log_reuse_status[db_file['DBName']] == 'NOTHING':
            shrinkable_log_files.append(db_file)
            
            if len(shrinkable_log_files) == int(max_log_files_count):
                break

    formatted_shrinkable_details = '\n'.join([str(i) for i in shrinkable_log_files])
    worknote = f'MSSQL log files identified for shrinking is:\n\n{formatted_shrinkable_details}'
    utils.update_worknote(ticket, worknote)

    shrinkable_log_files = json.dumps(shrinkable_log_files)
    print(f'#JOB_RESULT shrinkable_log_files = {shrinkable_log_files}')


if __name__ == '__main__':
    try:
        db_files = get_db_files()
        transaction_status = get_transaction_status()
        shrinkable_log_files = get_shrinkable_log_files(db_files, transaction_status)
    except:
        utils.error_occured(ticket, traceback.format_exc())
