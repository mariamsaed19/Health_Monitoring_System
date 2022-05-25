import json
from datetime import datetime
import time
import os
import csv
import pandas as pd
print('start...')

start_time = time.time()
msg_count = 0

data_path =  'D:/Manar Handasa/3rd year-2nd semester/Big Data Systems/project/input' # path of health_data directory
processed_path =  'D:/Manar Handasa/3rd year-2nd semester/Big Data Systems/project/csv_input' # path for output directory

#iterate over files in health_data directory
for filename in os.listdir(data_path):
    path = os.path.join(data_path, filename)
    print(filename)
    # now we will open a file for writing
    # data_file = open(processed_path + '/' + filename.rpartition('.')[0] + '.csv', 'w')
    obj_list = []
    
    # create the csv writer object
    #csv_writer = csv.writer(data_file)

    with open(path) as f:
        count = 0
        for line in f:
            line = line.replace("\n", "")
            json_obj = json.loads(line)
            obj_list.append(json_obj)
            # if count == 0:
            #     # Writing headers of CSV file
            #     header = json_obj.keys()
            #     #csv_writer.writerow(header)
            #     count += 1
        
            # # Writing data of CSV file
            # csv_writer.writerow(json_obj.values())
            msg_count += 1

            '''json_str = line.replace("'", "\"")
            json_str = json_str.replace("}{", "}\n{")
            json_str=json_str.split('\n')
            for st in json_str:
                j = json.loads(st)
                timestamp = j['Timestamp']
                dt_object = datetime.fromtimestamp(timestamp)
                filename = dt_object.strftime('%d_%m_%Y') + '.log'
                # add json to the dictionary
                if filename not in str_dic:
                    str_dic[filename] = []
                str_dic[filename].append(st+"\n")
                msg_count += 1'''

        # data_file.close()
        df = pd.json_normalize(obj_list)
        df.to_csv(processed_path + '/' + filename.rpartition('.')[0] + '.csv', index = False, header = False)

    '''#save collected jsons
    for k, v in str_dic.items():
        file =  open(os.path.join(processed_path, k),"a")
        file.writelines(v)
        file.close()'''
     
end_time = time.time()
print('total health messages :', msg_count)
print('latency :', end_time - start_time)
print('overall throuput:', msg_count / (end_time - start_time))
    


