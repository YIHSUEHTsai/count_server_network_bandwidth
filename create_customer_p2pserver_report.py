#!/usr/bin/python3
import os
import sys
import csv
import pandas as pd
import subprocess
import time
import datetime
import requests
import sqlite3
import math

file_path = "/home/ubuntu/script/p2pserver_report/"
sqlite_path = "/home/johnny/p2pRate/"
#file_path = "/home/ethan/Desktop/p2pserver_report/"
#sqlite_path = "/home/ethan/Desktop/p2pserver_report/"


customer_report_path = ""

prtg_username = "username=opapi"
prtg_pw = "passhash=4025917408"
prtg_url_jp="https://prtg-master.tutk.com/api/historicdata.csv?id=" #jp
prtg_url_us="https://prtg-slave.tutk.com/api/historicdata.csv?id="  #us

customer_name_list = []
p2p_server_list = []
p2p_server_suid_rate_list = []
p2p_server_prtg_bandwidth_id_list = []
p2p_server_prtg_devicelogin_id_list = []

def report_time_setting(start_time_month, start_time_day, end_time_month, end_time_day):
    time = datetime.datetime.now()
    year = time.year
    start_time_month = "%02d"%int(start_time_month)
    start_time_day = "%02d"%int(start_time_day)
    end_time_month = "%02d"%int(end_time_month)
    end_time_day = "%02d"%int(end_time_day)

    prtg_start_time = str(datetime.date(int(year), int(start_time_month), int(start_time_day))) + "-00-00"
    prtg_end_time = str(datetime.date(int(year), int(end_time_month), int(end_time_day)) + datetime.timedelta(days = 1)) + "-00-00"

    start_time = datetime.date(int(year), int(start_time_month), int(start_time_day))
    end_time = datetime.date(int(year), int(end_time_month), int(end_time_day))
    day_gap = end_time - start_time

    return prtg_start_time, prtg_end_time, start_time, end_time, day_gap

def reset_customer_report_folder(customer_name):
    cmd = "rm -rf " + file_path + customer_name + "/"
    subprocess.check_output(cmd, shell=True)

def create_customer_report_folder(customer_name):
    global customer_report_path

    cmd = "mkdir -p " + file_path + customer_name + "/"
    subprocess.check_output(cmd, shell=True)

    cmd = "mkdir -p " + file_path + customer_name + "/network_bandwidth/" 
    subprocess.check_output(cmd, shell=True)

    cmd = "mkdir -p " + file_path + customer_name + "/network_bandwidth/prtg/" 
    subprocess.check_output(cmd, shell=True)
    
    cmd = "mkdir -p " + file_path + customer_name + "/network_bandwidth/csv_document/" 
    subprocess.check_output(cmd, shell=True)

    customer_report_path = file_path + customer_name + "/"

def load_customer_name_list():
    p2p_server_list_csv = file_path + "p2p_list.csv"
    p2p_server_list_data = pd.read_csv(p2p_server_list_csv)

    for index in range(len(p2p_server_list_data)):
        customer_name = p2p_server_list_data['customer_id'][index]

        if (len(customer_name_list) == 0):
            customer_name_list.append(customer_name)
        else:
            for index in range(len(customer_name_list)):
                if (customer_name == customer_name_list[index]):
                    break
                elif (index == len(customer_name_list)-1):
                    customer_name_list.append(customer_name)

def load_p2p_server_list(customer_name):
    p2p_server_list_csv = file_path + "p2p_list.csv"
    p2p_server_list_data = pd.read_csv(p2p_server_list_csv)

    for index in range(len(p2p_server_list_data)):
        if (p2p_server_list_data['customer_id'][index] == customer_name):
            p2p_server_name = p2p_server_list_data['server_name'][index]

            p2p_server_list.append(p2p_server_name)

            p2p_server_prtg_bandwidth_id = p2p_server_list_data['bandwidth_id'][index]
            p2p_server_prtg_bandwidth_id_list.append(p2p_server_prtg_bandwidth_id)

            p2p_server_prtg_devicelogin_id = p2p_server_list_data['device_login_id'][index]
            p2p_server_prtg_devicelogin_id_list.append(p2p_server_prtg_devicelogin_id)

            p2p_server_suid_rate = p2p_server_list_data['suid_rate'][index]
            p2p_server_suid_rate_list.append(p2p_server_suid_rate)

    print ("[p2p server list]", p2p_server_list)
    print ("[prtg bandwidth id]", p2p_server_prtg_bandwidth_id_list)
    print ("[prtg device login id]", p2p_server_prtg_devicelogin_id_list)

def download_prtg_file(p2p_server_list, start_time_month, start_time_day, end_time_month, end_time_day):
    global customer_report_path

    prtg_start_time, prtg_end_time, start_time, end_time, day_gap = report_time_setting(start_time_month, start_time_day, end_time_month, end_time_day)

    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]
        bandwidth_prtg_id = p2p_server_prtg_bandwidth_id_list[index]

        get_prtg_bandwidth_file_url = prtg_url_us + str(bandwidth_prtg_id) + "&avg=0&sdate=" + prtg_start_time + "&edate=" + prtg_end_time + "&" + str(prtg_username) + "&" + str(prtg_pw)
        
        bandwidth_csv_file = requests.get(get_prtg_bandwidth_file_url)
        log_file_name = customer_report_path + "network_bandwidth/prtg/" + server_name + "_bandwidth_raw_tmp.csv"
        with open(log_file_name, 'wb') as f:
            f.write(bandwidth_csv_file.content)

        device_login_id = p2p_server_prtg_devicelogin_id_list[index]
        get_prtg_devicelogin_file_url = prtg_url_us + str(device_login_id) + "&avg=3600&sdate=" + prtg_start_time + "&edate=" + prtg_end_time + "&" + str(prtg_username) + "&" + str(prtg_pw)

        devicelogin_csv_file = requests.get(get_prtg_devicelogin_file_url)
        log_file_name = customer_report_path + "network_bandwidth/prtg/" + server_name + "_device_login.csv"
        with open(log_file_name, 'wb') as f:
            f.write(devicelogin_csv_file.content)

def reload_prtg_file(p2p_server_list):
    print ("reload prtg file...")
   
    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]
        print (server_name)
        csv_file  = open(customer_report_path + "network_bandwidth/prtg/" + server_name +"_bandwidth_raw_tmp2.csv", "w", newline='')
        p2pserver_connect_report_csv_writer = csv.writer(csv_file)
        p2pserver_connect_report_csv_writer.writerow(['Date', 'Traffic Out (speed)(RAW)'])

        csv_file = customer_report_path + "network_bandwidth/prtg/" + server_name +"_bandwidth_raw_tmp.csv"
        csv_file_data = pd.read_csv(csv_file)

        for csv_index in range(len(csv_file_data)-2):
            date = str(csv_file_data['Date Time'][csv_index]).split(" ", 3)[0]
            hour = str(str(csv_file_data['Date Time'][csv_index]).split(" ", 2)[1]).split(":",3)[0]
            time_zone = str(csv_file_data['Date Time'][csv_index]).split(" ", 3)[2]
            if (time_zone == "PM" and hour != "12"):
                hour = int(hour) + 12
            
            if (time_zone == "AM" and hour == "12"):
                hour = 0

            time = date + " " + str(hour).zfill(2)
            if math.isnan(csv_file_data['Traffic Out (speed)(RAW)'][csv_index]):
                traffic_out = 0
            else:
                traffic_out = csv_file_data['Traffic Out (speed)(RAW)'][csv_index]
            p2pserver_connect_report_csv_writer.writerow([time, traffic_out])

def create_bandwidth_report(p2p_server_list,start_time_month, start_time_day, end_time_month, end_time_day):
    print ("create bandwidth report...")

    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]

        csv_file  = open(customer_report_path + "network_bandwidth/prtg/" + server_name +"_bandwidth.csv", "w", newline='')
        p2pserver_connect_report_csv_writer = csv.writer(csv_file)
        p2pserver_connect_report_csv_writer.writerow(['Date', 'Traffic Out (speed)(RAW)'])

        csv_file = customer_report_path + "network_bandwidth/prtg/" + server_name + "_bandwidth_raw_tmp2.csv"
        csv_file_data = pd.read_csv(csv_file)

        prtg_start_time, prtg_end_time, start_time, end_time, day_gap = report_time_setting(start_time_month, start_time_day, end_time_month, end_time_day)

        for day in range(int(day_gap.days),-1,-1):
            time = end_time - datetime.timedelta(days=day)

            year = str(time).split("-",3)[0]
            month = str(time).split("-",3)[1]
            day = str(time).split("-",3)[2]

            for hour_index in range(24):
                date = str(int(month)) + "/" + str(int(day)) + "/" + year + " " + str("%02d" % hour_index)
                date =str(date)

                bandwidth_data_list = []
                for csv_index in range(len(csv_file_data)):
                    if (csv_file_data["Date"][csv_index] == date):
                        if (math.isnan(csv_file_data['Traffic Out (speed)(RAW)'][csv_index])):
                            print ("",end='')
                        else:
                            bandwidth_data_list.append(csv_file_data['Traffic Out (speed)(RAW)'][csv_index])

                if (len(bandwidth_data_list) != 0):
                    p2pserver_connect_report_csv_writer.writerow([date, max(bandwidth_data_list)])
                    #print (max(bandwidth_data_list))
                    bandwidth_data_list.clear()
                else:
                    p2pserver_connect_report_csv_writer.writerow([date, 0])

def create_p2pserver_connect_report(customer_name, start_time_month, start_time_day, end_time_month, end_time_day):
    global customer_report_path

    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]
        csv_file  = open(customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_p2pserver_connect_report_tmp.csv", "w", newline='')
        p2pserver_connect_report_csv_writer = csv.writer(csv_file)
        p2pserver_connect_report_csv_writer.writerow(['server_name', 'date', 'p2pcount', 'relaycount'])

        conn = sqlite3.connect(sqlite_path + 'p2prate.sqlite')
        c = conn.cursor()

        prtg_start_time, prtg_end_time, start_time, end_time, day_gap = report_time_setting(start_time_month, start_time_day, end_time_month, end_time_day)

        for day in range(int(day_gap.days),-1,-1):
            time = end_time - datetime.timedelta(days=day)
            
            sqlite_cmd = "select ServerName, YMDH, P2PCount, RelayCount from p2prate where ServerName = " + "'/TUTK/p2p/" + server_name + "/'" + "and YMDH like '%" + time.strftime("%Y-%m-%d") + "%' and Country = 'ALL';"
            cursor = c.execute(sqlite_cmd)

            for row in cursor:
                p2pserver_connect_report_csv_writer.writerow(row)

def p2pserver_connect_report_check_whether_have_null_value(customer_name, start_time_month, start_time_day, end_time_month, end_time_day):
    global customer_report_path

    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]
        print (server_name)
        csv_file  = open(customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_p2pserver_connect_report.csv", "w", newline='')
        p2pserver_connect_report_csv_writer = csv.writer(csv_file)
        p2pserver_connect_report_csv_writer.writerow(['server_name', 'date', 'p2pcount', 'relaycount'])

        prtg_start_time, prtg_end_time, start_time, end_time, day_gap = report_time_setting(start_time_month, start_time_day, end_time_month, end_time_day)

        data_index  = 0
        for day in range(int(day_gap.days),-1,-1):
            time = end_time - datetime.timedelta(days=day)

            for index in range(0,24):
                hour = "%02d"%int(index)
                time_check_value = time.strftime("%Y-%m-%d") + " " + str(hour)
                time_list = []
                time_list.append(time_check_value)

                p2pserver_connect_report_csv = customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_p2pserver_connect_report_tmp.csv"
                p2pserver_connect_report_data = pd.read_csv(p2pserver_connect_report_csv)

                df = p2pserver_connect_report_data[p2pserver_connect_report_data['date'].isin(time_list)]

                if (df.empty == True):
                    p2pserver_connect_report_csv_writer.writerow([p2pserver_connect_report_data['server_name'][0], time_check_value, 1.0, 1.0])
                else: 
                    p2pserver_connect_report_csv_writer.writerow([p2pserver_connect_report_data['server_name'][data_index], time_check_value, p2pserver_connect_report_data['p2pcount'][data_index], p2pserver_connect_report_data['relaycount'][data_index]])
                    data_index = data_index + 1

def create_network_bandwidth_tmp_report():
    global customer_report_path

    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]

        csv_file  = open(customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_network_bandwidth_report_tmp.csv", "w", newline='')
        network_bandwidth_report_csv_writer = csv.writer(csv_file)
        network_bandwidth_report_csv_writer.writerow(['date', 'Traffic Out (speed)(RAW)(Bytes)'])

        prtg_network_bandwidth_csv = customer_report_path + "network_bandwidth/prtg/" + server_name + "_bandwidth.csv"
        prtg_network_bandwidth_data = pd.read_csv(prtg_network_bandwidth_csv)

        for index in range(0,len(prtg_network_bandwidth_data)):
            network_bandwidth_report_csv_writer.writerow([prtg_network_bandwidth_data['Date'][index], prtg_network_bandwidth_data['Traffic Out (speed)(RAW)'][index]])

def create_device_login_report():
    global customer_report_path

    for server_index in range(len(p2p_server_list)):
        server_name = p2p_server_list[server_index]
        suid_rate = p2p_server_suid_rate_list[server_index]

        csv_file  = open(customer_report_path + "/network_bandwidth/csv_document/" + server_name + "_devicelogin.csv", "w", newline='')
        device_login_report_csv_writer = csv.writer(csv_file)

        header_name = server_name + ' No. of Login Devices'
        if (server_index == 0):
            device_login_report_csv_writer.writerow(['date', header_name])
        else:
            device_login_report_csv_writer.writerow([header_name])

        prtg_device_login_csv = customer_report_path + "network_bandwidth/prtg/" + server_name + "_device_login.csv"
        prtg_device_login_data = pd.read_csv(prtg_device_login_csv)

        for index in range(0, len(prtg_device_login_data)-2):
            if (server_index == 0):
                if math.isnan(prtg_device_login_data['No. of Login Devices(RAW)'][index]):
                    device_login_value = 0
                else:
                    device_login_value = prtg_device_login_data['No. of Login Devices(RAW)'][index] 
                #device_login_report_csv_writer.writerow([prtg_device_login_data['Date Time'][index], int(float(prtg_device_login_data['No. of Login Devices(RAW)'][index]) * float(suid_rate))])
                device_login_report_csv_writer.writerow([prtg_device_login_data['Date Time'][index], int(float(device_login_value) * float(suid_rate))])
            else:
                if math.isnan(prtg_device_login_data['No. of Login Devices(RAW)'][index]):
                    device_login_value = 0
                else:
                    device_login_value = prtg_device_login_data['No. of Login Devices(RAW)'][index]

                #device_login_report_csv_writer.writerow([int(float(prtg_device_login_data['No. of Login Devices(RAW)'][index]) * float(suid_rate))])
                device_login_report_csv_writer.writerow([int(float(device_login_value) * float(suid_rate))])

def create_device_login_final_report():
    global customer_report_path

    csv_file_list = []
    for index_1 in range(len(p2p_server_list)):
        server_name = p2p_server_list[index_1]
        print ("--->", server_name)
        csv_file = customer_report_path + "/network_bandwidth/csv_document/" + server_name + "_devicelogin.csv"
        csv_file_list.append(pd.read_csv(csv_file))

    merge_data = pd.concat(csv_file_list, axis=1, join_axes=None, verify_integrity=False)
    merge_data.to_csv(customer_report_path + "/network_bandwidth/csv_document/" + 'device_login_final_report.csv',index=False)
    csv_file_list.clear()

def create_network_bandwidth_report(customer_name, start_time_month, start_time_day, end_time_month, end_time_day):
    global customer_report_path

    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]
        suid_rate = p2p_server_suid_rate_list[index]
        print ("[server]",server_name)

        csv_file  = open(customer_report_path + "/network_bandwidth/csv_document/" + server_name + "_network_bandwidth_final_report.csv", "w", newline='')
        network_bandwidth_final_report_csv_writer = csv.writer(csv_file)

        if (index == 0):
            network_bandwidth_final_report_csv_writer.writerow(['date', server_name + '_network_bandwidth(Mbit/s)'])
        else:
            network_bandwidth_final_report_csv_writer.writerow([server_name + '_network_bandwidth(Mbit/s)'])
        
        network_bandwidth_report_csv = customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_network_bandwidth_report_tmp.csv"
        network_bandwidth_report_data = pd.read_csv(network_bandwidth_report_csv)

        p2pserver_connect_report_csv = customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_p2pserver_connect_report.csv"
        p2pserver_connect_report_data = pd.read_csv(p2pserver_connect_report_csv)

        total_network_bandwidth = 0
        average_network_bandwidth = 0
        for data_index in range(len(p2pserver_connect_report_data)):
            date = p2pserver_connect_report_data['date'][data_index]
            p2pcount = p2pserver_connect_report_data['p2pcount'][data_index]
            relaycount = p2pserver_connect_report_data['relaycount'][data_index]
            relaycount_rate = int(relaycount) / (int(p2pcount) + int(relaycount))

            network_bandwidth_bytes = network_bandwidth_report_data['Traffic Out (speed)(RAW)(Bytes)'][data_index]

            network_bandwidth = int(network_bandwidth_bytes) / float(round(relaycount_rate, 3))
            network_bandwidth = float(network_bandwidth) * 8 / 1000000
            network_bandwidth = network_bandwidth * float(suid_rate)

            #print (network_bandwidth)

            total_network_bandwidth = total_network_bandwidth + network_bandwidth

            if (index == 0):
                network_bandwidth_final_report_csv_writer.writerow([date, '%.8f' %  round(float(network_bandwidth),8)])
            else:
                network_bandwidth_final_report_csv_writer.writerow(['%.8f' %  round(float(network_bandwidth),8)])

        average_network_bandwidth = float(total_network_bandwidth) / int(len(p2pserver_connect_report_data))

        if (index == 0):
            network_bandwidth_final_report_csv_writer.writerow(["average", '%.8f' % round(float(average_network_bandwidth),8)])
        else:
            network_bandwidth_final_report_csv_writer.writerow(['%.8f' % round(float(average_network_bandwidth),8)])

def create_all_p2pserver_total_network_bandwidth():
    csv_file  = open(customer_report_path + "/network_bandwidth/csv_document/" + 'all_p2pserver_total_network_bandwidth_report.csv', "w", newline='')
    network_bandwidth_final_report_csv_writer = csv.writer(csv_file)
    network_bandwidth_final_report_csv_writer.writerow(['total_average_network_bandwidth(Mbit/s)'])

    csv_file_list = []
    for index in range(len(p2p_server_list)):
        server_name = p2p_server_list[index]
        csv_file = customer_report_path + "/network_bandwidth/csv_document/" + server_name +"_network_bandwidth_final_report.csv"
        csv_file_list.append(pd.read_csv(csv_file))

    merge_data = pd.concat(csv_file_list, axis=1, join_axes=None, verify_integrity=False)
    merge_data.to_csv(customer_report_path + "/network_bandwidth/csv_document/" + 'network_bandwidth_final_report_tmp.csv',index=False)
    csv_file_list.clear()
   
    network_bandwidth_final_report_tmp_csv = customer_report_path + "/network_bandwidth/csv_document/" + 'network_bandwidth_final_report_tmp.csv'
    network_bandwidth_final_report_tmp_csv_data = pd.read_csv(network_bandwidth_final_report_tmp_csv)

    total_bandwidth_value_for_all_p2pserver = 0
    for index in range(len(network_bandwidth_final_report_tmp_csv_data)):
        for server_index in range(len(p2p_server_list)):
            server_name = p2p_server_list[server_index]
            header_name = server_name + "_network_bandwidth(Mbit/s)"
            total_bandwidth_value_for_all_p2pserver = total_bandwidth_value_for_all_p2pserver + float(network_bandwidth_final_report_tmp_csv_data[header_name][index])

        network_bandwidth_final_report_csv_writer.writerow(['%.8f' %  round(float(total_bandwidth_value_for_all_p2pserver),8)])

        total_bandwidth_value_for_all_p2pserver = 0

def create_network_bandwidth_final_report():
    csv_file_list = []
    #merge csv
    csv_file = customer_report_path + "/network_bandwidth/csv_document/network_bandwidth_final_report_tmp.csv"
    csv_file_list.append(pd.read_csv(csv_file))

    csv_file = customer_report_path + "/network_bandwidth/csv_document/all_p2pserver_total_network_bandwidth_report.csv"
    csv_file_list.append(pd.read_csv(csv_file))

    merge_data = pd.concat(csv_file_list, axis=1, join_axes=None, verify_integrity=False)
    merge_data.to_csv(customer_report_path + "/network_bandwidth/csv_document/" + 'network_bandwidth_final_report.csv',index=False)

def create_max_bandwidth_value():
    csv_file = customer_report_path + "/network_bandwidth/csv_document/" + 'network_bandwidth_final_report.csv'
    open_csv_file  = open(csv_file, "a", newline='')
    csv_file_writer = csv.writer(open_csv_file)

    data = pd.read_csv(csv_file)

    max_bandwidth_value_list = ["max"]
    total_max_bandwidth = 0
    for server_index in range(len(p2p_server_list)):
        server_name = p2p_server_list[server_index]

        header_name = server_name + "_network_bandwidth(Mbit/s)"
        max_bandwidth = float(data[header_name].max())
        total_max_bandwidth = total_max_bandwidth + max_bandwidth
        max_bandwidth_value_list.append(max_bandwidth)

    max_bandwidth_value_list.append(total_max_bandwidth)

    csv_file_writer.writerow(max_bandwidth_value_list)

if __name__ == "__main__":
    load_customer_name_list()

    while True:
        try:
            print ("\033[0;31m%s\033[0m" % "[Customer name]")
            for index in range(len(customer_name_list)):
                print ("\033[0;31m%s\033[0m" % customer_name_list[index])

            customer_name = str(input("\033[0;34m%s\033[0m" % "Please input customer name: "))

            print ("\033[0;31m%s\033[0m" % "[Please input you want to parser time range]")
            print ("\033[0;31m%s\033[0m" % "[Please input start time]")
            start_time_month = str(input("\033[0;34m%s\033[0m" % "Month: "))
            start_time_day = str(input("\033[0;34m%s\033[0m" % "Day: "))

            print ("\033[0;31m%s\033[0m" % "[Please input end time]")
            end_time_month = str(input("\033[0;34m%s\033[0m" % "Month: "))
            end_time_day = str(input("\033[0;34m%s\033[0m" % "Day: "))

        except ValueError:
            continue
        else:
            break

    #reset_customer_report_folder(customer_name)
    create_customer_report_folder(customer_name)
    load_p2p_server_list(customer_name)
    download_prtg_file(p2p_server_list, start_time_month, start_time_day, end_time_month, end_time_day)
    reload_prtg_file(p2p_server_list)
    create_bandwidth_report(p2p_server_list,start_time_month, start_time_day, end_time_month, end_time_day)

    #//create report
    create_p2pserver_connect_report(customer_name, start_time_month, start_time_day, end_time_month, end_time_day)
    p2pserver_connect_report_check_whether_have_null_value(customer_name, start_time_month, start_time_day, end_time_month, end_time_day)
    create_network_bandwidth_tmp_report()
    #create_device_login_report()
    #create_device_login_final_report()

    #//creapt final report
    create_network_bandwidth_report(customer_name, start_time_month, start_time_day, end_time_month, end_time_day)
    create_all_p2pserver_total_network_bandwidth()
    create_network_bandwidth_final_report()
    create_max_bandwidth_value()
