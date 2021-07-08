#!/usr/bin/env python
# coding: utf-8

# # Import libraries and modules
# Basics
import logging
import pandas as pd

# Inbox lib [mine]
from inbox import *

# Pymatgen
from pymatgen.io.vasp import inputs
from pymatgen.io.vasp import sets

# # Instantinate log
logging.basicConfig(filename="Mail_Reader_Home_Work_1_rel2.log", level=logging.INFO, filemode='a')
log = logging.getLogger("main_log")

VERSION = '0426.1533' ; log.info(f'Version: {VERSION}')
log.info(f'-STARTED-----------------------------------------------------')

# # Load & Update history
hist_file = 'HW1_hist.csv'
hist_init = get_hist(hist_file)

hist_init.to_csv(hist_file, index=False)
log.info(f'Database {hist_file} updated')

# # Update history for newbies
# 1. Check  poscar
hist = check_incoming_poscars(hist_init)
hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with checked poscars')

# 2. Compose calculation input files as strings for newbie
hist = prepare_files(hist)
hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with input files')

# # Submit newbies' calculations
# Filter available calcs
df_to_launch = hist[hist['files_prepared'] == 'YES']
df_to_launch = df_to_launch[df_to_launch['submitted'] == 'NO']

# Paths on server
path = '/home/k.sidnov/Calculations/Practics/2021'
path_to_potpaw = '/home/k.sidnov/VASP_potential/potpaw_PBE.54'

# Create script
# SSH command
def command(inputs):
    stdin, stdout, stderr = client.exec_command(inputs)
    raw_data = stdout.read() + stderr.read() #; log.info(raw_data)
    end_data = str(raw_data).replace("\\n", "\n").replace("b'", "").replace("'","")
    return end_data

# # Cluster part
import paramiko
import datetime

try:
    client.close()
    sftp.close()
    transport.close()
    log.info(f'All channels closed at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
except Exception as e:
    if "name 'client' is not defined" in str(e):
        log.info('Session is not opened yet. It will be opened next')
    elif "name 'sftp' is not defined" in str(e):
        log.info('SFTP channel is not opened yet. It will be opened next')
    else:
        log.info(e)

# Logging in data
host = '**.*.**.***'
port = '22'
user = '********'
password = '********'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=host, username=user, password=password, port=port)
log.info(f'SSH session opened at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
log.info('---')

# Launch news
for idx in df_to_launch.index:
    data = df_to_launch.loc[idx]
    name = data['name']
    path = path
    id_num = data['time']
    
    INCAR, KPOINTS, POSCAR, POTPAW, sbatch = data['INCAR'], data['KPOINTS'], data['POSCAR'], data['POTPAW'], data['sbatch']
    log.info(command(shell_get_dir(name, path, id_num)))
    log.info(command(shell_send_infiles(name, path, id_num, INCAR, KPOINTS, POSCAR, POTPAW, sbatch)))
    
    log.info(command(shell_job_submitting(name, path, id_num)))
    hist.loc[idx, 'submitted'] = 'YES'
    hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with submitted calcs')
    
# check results
transport = paramiko.Transport((host, int(port)))
transport.connect(username=user, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)
log.info(f'SFTP channel opened at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
    
# check job satate
for idx in hist.index:
    data = hist.loc[idx]
    
    id_num = data['time']
    name = data['name']
    email = data['email']
    submitted = data['submitted']
    job_name = data['job_name']
    done = data['done']
    result_sent = data['result_sent']
    path = path
    
    if submitted == 'YES' and done != 'YES':
        result = command(shell_check_job_state(job_name, path, name, id_num))
        if 'done' in result:
            hist.loc[idx, 'done'] = 'YES'
            hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with done calcs')
        elif 'pending' in result:
            hist.loc[idx, 'done'] = 'PENDING'
            hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with pending calcs')
        elif 'running' in result:
            hist.loc[idx, 'done'] = 'RUNNING'
            hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with running calcs')
        elif 'error' in result:
            hist.loc[idx, 'done'] = 'ERROR'
            hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with error while calc')

    if done == 'YES' and result_sent == 'NO':
        # get output files
        remotepath = f'{path}/{name}/{id_num}/{job_name}_vasp_out.zip'
        log.info(f'Remotepath: {remotepath}')
        sftp.get(remotepath, f'{job_name}_vasp_out.zip')
        hist.loc[idx, 'result_sent'] = 'DOWNLOADED'
        hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with downloaded files')
    
    if done == 'YES' and result_sent == 'DOWNLOADED':
        init_text = 'Это письмо отправлено автоматически, т.к. Вы заполнили форму для выполнения Домашнего задания №1. Отвечать на это письмо не нужно.'
        subject = 'ПМО для дизайна новых материалов. Домашнее задание №1'
        postscriptum = f'\n--- \nSent by "HW1" Ver.:{VERSION}'
        is_file = True
        filename = f'{job_name}_vasp_out.zip'
        text = f'Task №{id_num} complete. Output files are attached to the letter.'
        mail_notification(init_text, subject, text, postscriptum, email, is_file, filename)
        
        log.info(f'Email: {text}')
        log.info(f'Notification sent to {email}')
        
        hist.loc[idx, 'result_sent'] = 'YES'
        hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with sent email')
        
sftp.close()
transport.close()
log.info(f'---\nSFTP channel closed at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
client.close()
log.info(f'SSH session closed at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')

# ---
# # Cases for notifications
# 1. Bad poscar `hist['poscar_check'] == 'BAD'`
# 2. Job submitted
# 3. Job done
# 4. Error while calculation occured

init_text = 'This email was sent automatically because You have submitted a request for calculation. You do not need to reply to this letter.'
subject = 'Application Areas of Database Analysis: Design of New Materials.'
postscriptum = f'\n--- \nSent by "HW1" Ver.:{VERSION}'

for idx in hist.index:
    data = hist.loc[idx]
   
    id_num = data['time']
    name = data['name']
    email = data['email']
    poscar_check = data['poscar_check']
    submitted = data['submitted']
    job_name = data['job_name']
    done = data['done']
    result_sent = data['result_sent']
    path = path
    
    # CASE 1. BAD POSCAR
    if poscar_check == 'BAD' and result_sent != 'YES':
        is_file = False
        filename = 'None'
        text = f'Task {id_num}: in POSCAR file an error occured. Task was aborted.'
        mail_notification(init_text, subject, text, postscriptum, email, is_file, filename)
        
        log.info(f'Email: {text}')
        log.info(f'Notification sent to {email}')
        
        hist.loc[idx, 'result_sent'] = 'YES'
        hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with sent email')
        
        continue
        
    # CASE 2. JOB SUBMITTED
    if poscar_check == 'BAD' and result_sent != 'YES':
        is_file = False
        filename = 'None'
        text = f'Задача {id_num}: in POSCAR file an error occured. Task was aborted.'
        mail_notification(init_text, subject, text, postscriptum, email, is_file, filename)
        
        log.info(f'Email: {text}')
        log.info(f'Notification sent to {email}')
        
        hist.loc[idx, 'result_sent'] = 'YES'
        hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with sent email')
        
        continue
        
    # CASE 3. SEE previous part
    # CASE 4. ERROR while calc
    if done == 'ERROR' and result_sent != 'YES':
        is_file = False
        filename = 'None'
        text = f'Задача {id_num}: An unexpected error occurred during the calculation.'
        mail_notification(init_text, subject, text, postscriptum, email, is_file, filename)
        
        log.info(f'Email: {text}')
        log.info(f'Notification sent to {email}')
        
        hist.loc[idx, 'result_sent'] = 'YES'
        hist.to_csv(hist_file, index=False) ; log.info(f'Database {hist_file} updated with sent email')
        
        continue

log.info(f'-STOPPED-----------------------------------------------------')
