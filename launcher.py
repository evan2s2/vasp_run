#!/usr/bin/env python
# coding: utf-8

import time
import datetime
import subprocess

print('--------------------------------------------------------------')
print(f'Work is started at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')

dt = datetime.datetime.now().strftime("%d-%m-%Y %H")

till = '26-04-2021 17'

command = "C:/Users/User/Anaconda3/python Mail_reader_HW1_release_2.py"

while dt != till:
    dt = datetime.datetime.now().strftime("%d-%m-%Y %H")
    my_subprocess = subprocess.Popen(command)
    print(f'Script will be active till {till}:00')
    print(f'Checked at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
    time.sleep(60)

print(f'Work is finished at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
print('--------------------------------------------------------------')
