import json
from datetime import datetime
import time
import os
print('start...')

start_time = time.time()
msg_count = 0

data_path =  # path of health_data directory
processed_path =  # path for output directory

#iterate over files in health_data directory
for filename in os.listdir(data_path):
    path = os.path.join(data_path, filename)
    print(filename)

    str_dic = {}    #to save jsons mapped to each file
    with open(path) as f:
        for line in f:
            json_str = line.replace("'", "\"")
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
                msg_count += 1


    #save collected jsons
    for k, v in str_dic.items():
        file =  open(os.path.join(processed_path, k),"a")
        file.writelines(v)
        file.close()
     
end_time = time.time()
print('total health messages :', msg_count)
print('latency :', end_time - start_time)
print('overall throuput:', msg_count / (end_time - start_time))
    


