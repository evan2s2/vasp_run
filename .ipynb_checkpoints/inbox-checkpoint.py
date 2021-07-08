import pandas as pd
import numpy as np
import os.path
import imaplib
import email

# PYMATGEN
from pymatgen.io.vasp import inputs
from pymatgen.io.vasp import sets
from pymatgen.core import Structure

# GET INBOX
def get_df_content(mail_content):
    all_content = mail_content.replace('\r', '').rstrip('\n') #; log.info(f'All: {all_content}')
    time_stmp = all_content.split('@&time:')[1].split('@&name:')[0] #; log.info(f'Time: {time}')
    name = all_content.split('@&name:')[1].split('@&email:')[0]
    email_addr = all_content.split('@&email:')[1].split('@&poscar:')[0]
    poscar = all_content.split('@&poscar:')[1].split('@&ctype:')[0]
    calc_type = all_content.split('@&ctype:')[1]
    
    return time_stmp, name, email_addr, poscar, calc_type

def get_hist(hist_file):
    file_exsist = os.path.isfile(hist_file)

    if not file_exsist:
        print(f'Hist file {hist_file} is not exist')
        columns = ['time', 'name', 'email', 'poscar', 'calc_type', 'poscar_check', 'submitted', 'done', 'result_sent', 'sbatch', 'job_name', 'POSCAR', 'POTPAW', 'INCAR', 'KPOINTS', 'files_prepared']
        hist = pd.DataFrame(columns=columns)
        print(f'Hist file {hist_file} will be created')
    else:
        hist = pd.read_csv(hist_file)

        #bool_columns = ['poscar_check','submitted','done','result_sent', 'files_prepared']
        #for idx in hist.index:
        #    for col in bool_columns:
        #        if hist.loc[idx, col].notna:
        #            hist.loc[idx, col] = hist.loc[idx, col].astype(bool)
        print(f'Hist file {hist_file} was found')

    EMAIL = '****@*****.**'
    PASSWORD = '**************'
    SERVER = '*************'

    mail = imaplib.IMAP4_SSL(SERVER)
    mail.login(EMAIL, PASSWORD)
    mail.select('inbox')
    status, data = mail.search(None, 'ALL')
    mail_ids = []
    for block in data:
        mail_ids += block.split()

    for i in mail_ids:
        status, data = mail.fetch(i, '(RFC822)')

        for response_part in data:
            if isinstance(response_part, tuple):
                message = email.message_from_bytes(response_part[1])
                mail_from = message['from']
                mail_subject = message['subject']

                if message.is_multipart():
                    mail_content = ''

                    for part in message.get_payload():
                        if part.get_content_type() == 'text/plain':
                            mail_content += part.get_payload()
                else:
                    mail_content = message.get_payload()

                if 'HomeWork_1' in mail_subject and mail_from == 'evan2s2s2@gmail.com':
                    print(f'From: {mail_from} / Subject: {mail_subject}')

                    try:
                        time_stmp, name, email_addr, poscar, calc_type = get_df_content(mail_content)
                    except Exception as e:
                        print(e)
                        continue

                    uni_index = int(time_stmp) #; log.info(uni_index)

                    # SET COLUMNS IF NOT IN DATAFRAME
                    isitin = hist.time.isin([uni_index]).any() #; log.info(isitin)

                    if not isitin:
                        if hist.index.shape[0] == 0:
                            idx = int(0) #; log.info(idx)
                        else:
                            idx = int(hist.index.max() + 1) #; log.info(idx)

                        hist.loc[idx] = [uni_index, name, email_addr, poscar, calc_type, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]

                    else:
                        print('This is an old reply \n---')

    return hist

# CHECKING POSCARS
def check_incoming_poscars(hist_init):
    hist = hist_init.copy()
    for i in hist[hist.poscar_check.isnull()].index:
        POSCAR = hist.loc[i, 'poscar']
        try:
            struct = inputs.Poscar.from_string(POSCAR)
            hist.loc[i, 'poscar_check'] = 'YES'
        except Exception as e:
            hist.loc[i, 'poscar_check'] = 'BAD'
    return hist

# POTCAR
def get_elems(struct):
    unique = []
    for i in struct.structure.species:
        if str(i) in unique:
            continue
        else:
            unique.append(str(i))
    return unique

Default_PAW = pd.read_csv('Default_PAW_potentials_VASP.csv', sep=';')
## SBATCH FILE
def sbatch_file(name, id_num):
    job_name = name + '_' + str(id_num)
    file_text =f'''#!/bin/bash -l
                #SBATCH -N 1
                #SBATCH -t 0-01:00:00
                #SBATCH -J {job_name}
                #SBATCH -p GPU-part
                #SBATCH --propagate=STACK
                module add intel/parallel_studio/2017.1.043
                module add soft/vasp/5.4.4-intel
                mpirun /home/bmp/calculations/vasp/Re-C/PBE/ReC2/ABAA/vasp_std
                '''
    return file_text.replace('                ', ''), job_name

# ALL FILES PREPARATION
def prepare_files(hist):
    for idx in hist[hist.poscar_check == 'YES'].index:

        if ('files_prepared' in hist.columns) and (hist.loc[idx, 'files_prepared'] == 'YES'):
            continue

        hist.loc[idx, 'submitted'] = 'NO'
        hist.loc[idx, 'done'] = 'NO'
        hist.loc[idx, 'result_sent'] = 'NO'

        id_num = hist.loc[idx,'time']
        name = hist.loc[idx, 'name']
        sbatch = sbatch_file(name, id_num)

        hist.loc[idx, 'sbatch'] = sbatch[0]
        hist.loc[idx, 'job_name'] = sbatch[1]

        # initial poscar
        POSCAR = hist.loc[idx, 'poscar']

        # as type struct -> trouble-free form of poscar
        struct = inputs.Poscar.from_string(POSCAR)
        
        # potentials as string from set of default PAW potential for VASP
        elements = get_elems(struct)
        PAW = []
        for elem in elements:
            PAW.append(Default_PAW[Default_PAW['Element'] == elem]['Element_(and_appendix)'].values[0])
        hist.loc[idx, 'POTPAW'] = str(PAW)

        # define calculation type [rx or elasticity]
        calc_type = hist.loc[idx, 'calc_type']

        # Cases for rx and elasticity
        if calc_type == 'rx':
            # files as strings
            INCAR = sets.MPRelaxSet(struct.structure).incar.get_string()
            KPOINTS = sets.MPRelaxSet(struct.structure).kpoints
            POSCAR = sets.MPRelaxSet(struct.structure).poscar.get_string()
                    
            # update hist
            hist.loc[idx, 'INCAR'] = INCAR
            hist.loc[idx, 'KPOINTS'] = str(KPOINTS)
            hist.loc[idx, 'POSCAR'] = POSCAR
            
        elif calc_type == 'elastic':
            # files as strings
            INCAR = sets.MVLElasticSet(struct.structure).incar.get_string()
            KPOINTS = sets.MVLElasticSet(struct.structure).kpoints
            POSCAR = sets.MVLElasticSet(struct.structure).poscar.get_string()
            
            # update hist
            hist.loc[idx, 'INCAR'] = INCAR
            hist.loc[idx, 'KPOINTS'] = str(KPOINTS)
            hist.loc[idx, 'POSCAR'] = POSCAR
            
        hist.loc[idx, 'files_prepared'] = 'YES'

    return hist


## SHELL COMMANDS
def shell_get_dir(name, path, id_num):
    dir_exist = f'''
        cd {path}
        if ! test -d {name} ; then 
            mkdir {name}
            echo 'Directory {name} created'
        else
            echo 'Directory {name} exists'
        fi
        
        cd {path}/{name}
        
        if test -d {id_num} ; then
            rm -r {id_num}
        fi
        mkdir {id_num}'''

    return dir_exist

def shell_job_submitting(name, path, id_num):
    submis_com =f'''
        cd {path}/{name}/{id_num}
        sbatch cherrystart'''

    return submis_com

def shell_send_infiles(name, path, id_num, INCAR, KPOINTS, POSCAR, POTPAW, sbatch):
    files_com = f'''
        cd {path}/{name}/{id_num}
        echo '{INCAR}' > INCAR
        echo '{KPOINTS}' > KPOINTS
        echo '{POSCAR}' > POSCAR
        echo '{sbatch}' > cherrystart'''

    pot_list = eval(POTPAW)
    path_to_potpaw = '/home/k.sidnov/VASP_potential/potpaw_PBE.54' ; print(f'Path to POTPAW is: {path_to_potpaw}')
    
    for pot in pot_list:
        pot_tmp_name = 'POTCAR_' + pot
        POTCAR_cat_command =f'''cp {path_to_potpaw}/{pot}/POTCAR {path}/{name}/{id_num}/{pot_tmp_name}'''
        files_com += '\n'
        files_com += POTCAR_cat_command

    concat_com = 'cat '
    for pot in pot_list:
        concat_com += 'POTCAR_' + pot + ' '
        
    concat_com += '> POTCAR'
    potcar_com =f'''
        cd {path}/{name}/{id_num}
        {concat_com}'''
    files_com += '\n'
    files_com += potcar_com

    return files_com

def shell_check_job_state(job_name, path, name, id_num):
    check_com = f'''cd {path}/{name}/{id_num}
                    if squeue -u k.sidnov -o "%.7i %.9P %.20j %.8u %.8T %.9M %.9l %.6D %R" | grep -q {job_name} ; then
                        if squeue -u k.sidnov -o "%.7i %.9P %.20j %.8u %.8T %.9M %.9l %.6D %R" | grep {job_name} | grep -q 'RUNNING' ; then
                            echo 'running'
                        else
                            if squeue -u k.sidnov -o "%.7i %.9P %.20j %.8u %.8T %.9M %.9l %.6D %R" | grep {job_name} | grep -q 'PENDING' ; then
                                echo 'pending'
                            fi
                        fi
                    else
                        if ( test -f OSZICAR ) && ( test -f *.out ) && ( test -f CONTCAR ) && ( test -f OUTCAR ) && ( grep -q 'Total CPU' OUTCAR ); then 
                            zip {job_name}_vasp_out.zip OSZICAR CONTCAR OUTCAR *.out
                            echo 'done'
                        else
                            echo 'error'
                        fi
                    fi'''
    return check_com

## SEND AN EMAIL WITH ATTACHMENT
import smtplib  # Импортируем библиотеку по работе с SMTP
# Добавляем необходимые подклассы - MIME-типы
from email.mime.multipart import MIMEMultipart  # Многокомпонентный объект
from email.mime.text import MIMEText  # Текст
from email.mime.base import MIMEBase
import email.mime.application
from email import encoders
    
def mail_notification(init_text, subject, text, postscriptum, email_addr, is_file, filename):
    addr_from = "HPC_mailer_daemon@inbox.ru"
    password = "cxz@18Wv@illegalmail"  # пароль от почты addr_from

    msg = MIMEMultipart()  # Создаем сообщение
    msg['From'] = addr_from  # Адресат
    msg['To'] = email_addr  # Получатель
    msg['Subject'] = subject  # Тема сообщения
    
    if is_file:
        # Compose attachment
        # PDF attachment
        fp = open(filename,'rb')
        att = email.mime.application.MIMEApplication(fp.read(),_subtype="pdf")
        fp.close()
        att.add_header('Content-Disposition','attachment',filename=filename)
        msg.attach(att)

    body = f'{init_text} \n \n{text} \n{postscriptum}'
    msg.attach(MIMEText(body, 'plain'))  # Добавляем в сообщение текст

    server = smtplib.SMTP_SSL('smtp.mail.ru', 465)  # Создаем объект SMTP
    # server.starttls()             # Начинаем шифрованный обмен по TLS
    server.login(addr_from, password)  # Получаем доступ
    server.send_message(msg)  # Отправляем сообщение
    server.quit()  # Выходим
