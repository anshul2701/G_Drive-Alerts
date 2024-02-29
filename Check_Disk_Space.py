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
threshold_percentage = configuration['counts']['G_drive_threshold']
winrm_session = winrm.Session(jump_server, auth=(username, password), transport='credssp', server_cert_validation='ignore')


def check_disk_space():
    if '.corp.riotinto.org' in host:
        target_server = host
    else:
        target_server = f'{host}.corp.riotinto.org'

    ps_script = '''
$Computer = "''' + target_server + '''"

$Disks = Get-wmiobject  Win32_LogicalDisk -computername $Computer -ErrorAction SilentlyContinue -filter "DriveType= 3"
$Servername = (Get-wmiobject  CIM_ComputerSystem -ComputerName $computer).Name

foreach ($objdisk in $Disks)  
{
    if ($objdisk.DeviceID -eq "G:")
    {     
        $total=“{0:N3}” -f ($objDisk.Size/1GB)  
        $free= “{0:N3}” -f ($objDisk.FreeSpace/1GB)  
        $freePercent="{0:P2}" -f ([double]$objDisk.FreeSpace/[double]$objDisk.Size)  
 
            $out=New-Object PSObject 
            $out | Add-Member -MemberType NoteProperty -Name "Servername" -Value $Servername 
            $out | Add-Member -MemberType NoteProperty -Name "Drive" -Value $objDisk.DeviceID  
            $out | Add-Member -MemberType NoteProperty -Name "Total size (GB)" -Value $total 
            $out | Add-Member -MemberType NoteProperty -Name “Free Space (GB)” -Value $free 
            $out | Add-Member -MemberType NoteProperty -Name “Free Space (%)” -Value $freePercent 
            $out | Add-Member -MemberType NoteProperty -Name "Name " -Value $objdisk.volumename 
            $out | Add-Member -MemberType NoteProperty -Name "DriveType" -Value $objdisk.DriveType 
            $out
    }
}
'''

    ps_response = winrm_session.run_ps(ps_script)
        
    if ps_response.status_code == 0:
        print(f'Disk space details fetched successfully from {target_server}')
        print(ps_response.std_out)

        worknote = f'After shrinking G: drive free space details:\n\n{ps_response.std_out.decode()}'
        utils.update_worknote(ticket, worknote)

        disk_space = utils.format_ps_output(ticket, ps_response.std_out.decode())
        
    else:
        print(f'Could not fetch disk space details from {target_server}')
        print(ps_response.std_err)
        utils.error_occured(ticket, 'error')

    branch = 0

    if (100 - float(disk_space[0]['Free Space (%)'].replace('%', ''))) < float(threshold_percentage):
        branch = 1
    else:
        branch = 2

        worknote = 'G: drive occupied disk space is not below threshold after completing log file shrinking process'
        print(f'#JOB_RESULT worknote = {worknote}')

    print(f'#JOB_RESULT branch = {branch}')


if __name__=='__main__':
    try:
        check_disk_space()
    except:
        utils.error_occured(ticket, traceback.format_exc())
