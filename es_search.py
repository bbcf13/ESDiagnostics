def color_string(result):
    if result=='PASS':
        color = 'green'        
    elif(result=='WARNING'):
        color = 'orange'        
    elif (result=='FAIL'):
        color = 'red'        
    else:
        color = 'black'
    return 'color: %s' % color

def red_color(data): 
    if(type(data)==str):
        if "/" in data:
            store_list = data.split("/")
            if(len(store_list)<=2):
                used = parse_bytes(store_list[0])
                total = parse_bytes(store_list[1])
                if(used > 0.8*total):
                    color = 'red'
                else:
                    color = 'black'
            elif(len(store_list)>2):
                total = int(store_list[0].split(":")[1])
                active = int(store_list[1].split(":")[1])
                queue = int(store_list[2].split(":")[1])
                if(active > 0.8*total):
                    color = 'red'
                else:
                    color = 'black'
            return 'color: %s' % color    
    
def highlight(df): 
    if df.Configuration == 'Basic Configuration' or df.Configuration == 'OS and JVM' or df.Configuration == 'Threadpool Usage':
        return ['background-color :	#0066CC']*(df.size)
    else:
        return ['background-color : white']*(df.size)

def ClusterConfiguration(nodes,cat_nodes):
    nodes_count = nodes["_nodes"]["total"]
    df = pandas.DataFrame(columns = ['Configuration'])
    row_header = ["Basic Configuration","Build flavor","Roles","Current Master","OS and JVM","OS Name", "Version","Arch","Processors","JVM Version","Heap size min","Heap size max"]
    for row_index in row_header:
        df.loc[len(df)] = row_index
    for node in nodes["nodes"]:
        blank = " "
        name = nodes["nodes"][node]["name"]
        build_flavor = nodes["nodes"][node]["build_flavor"]
        roles = " , ".join(nodes["nodes"][node]["roles"])
        for cat_node in cat_nodes:
            if(cat_node["name"]==name):
                if(cat_node["master"]=="*"):
                    master = 'Yes'
                else:
                    master = "No"
                break
        os_name = nodes["nodes"][node]["os"]["name"]
        os_version = nodes["nodes"][node]["os"]["version"]
        os_arch = nodes["nodes"][node]["os"]["arch"]
        os_processors = nodes["nodes"][node]["os"]["available_processors"]
        jvm_version = nodes["nodes"][node]["jvm"]["version"]
        jvm_heap_min = str(int((nodes["nodes"][node]["jvm"]["mem"]["heap_init_in_bytes"])/(1024**3))) +'GB'
        jvm_heap_max = str(int((nodes["nodes"][node]["jvm"]["mem"]["heap_max_in_bytes"])/(1024**3)))+'GB'
        
        df[name] = [blank,build_flavor,roles,master,blank,os_name,os_version,os_arch,os_processors,jvm_version,jvm_heap_min,jvm_heap_max]
 
    styles = [dict(selector = '',props = [("text-align", "center"),('border','2px solid blue'),('background-color', 'white'),('border-color', 'black')]),    
            dict(selector= 'th', props= [('font-size', '12px'),('border-style' ,'solid'),('height',"30px"),('border-width', '0px'),('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'),("font-weight", "normal"), ('vertical-align', 'centre')]),
            dict(selector= "tbody td", props= [("border", "1px solid grey"),('font-size', '12px'),('border-width', '0.5px')])]

    df = df.style.set_table_styles(styles).set_properties(subset=df.columns, **{'width': '100px'}).hide_index()
    dfnew = df.apply(highlight, axis = 1)
    return dfnew
    
def OverallStats(nodes_stats,cat_allocation):   
    disk_data = []
    nodes_count = nodes_stats["_nodes"]["total"]
    df = pandas.DataFrame(columns = ['Configuration'])
    row_list = ["Store size","CPU 15min Load Average","Memory Usage","swap","Disk Usage","Threadpool Usage","search", "write","get","refresh","snapshot","management"]
    for row_index in row_list:
        df.loc[len(df)] = row_index
    for node in nodes_stats["nodes"]:
        blank = " "
        name = nodes_stats["nodes"][node]["name"]
        store_size = str(int((nodes_stats["nodes"][node]["indices"]["store"]["size_in_bytes"])/(1024**3))) +'GB'
        cpu_15minloadaverage = "{:0.2f}".format(nodes_stats["nodes"][node]["os"]["cpu"]["load_average"]["15m"])
        memory_total = str(int((nodes_stats["nodes"][node]["os"]["mem"]["total_in_bytes"])/(1024**3))) +'GB'
        memory_usage = str(int((nodes_stats["nodes"][node]["os"]["mem"]["used_in_bytes"])/(1024**3))) +'GB'
        memory = memory_usage + "/" + memory_total
        swap = nodes_stats["nodes"][node]["os"]["swap"]["total_in_bytes"]
        for cat_node in cat_allocation:
            if(cat_node["node"]==name):
                disk_used = cat_node["disk.used"]
                disk_total = cat_node["disk.total"]  
                disk_data.append([cat_node["node"],disk_used.upper(),disk_total.upper()])
                break
        disk = disk_used.upper() + "/" + disk_total.upper()
        thread_pool_list = ["search","write","get","refresh","snapshot","management"] 
        column_data = [store_size,cpu_15minloadaverage,memory,swap,disk,blank]
        for thread_pool in thread_pool_list:
            threads = nodes_stats["nodes"][node]["thread_pool"][thread_pool]["threads"]
            queue = nodes_stats["nodes"][node]["thread_pool"][thread_pool]["queue"]
            active = nodes_stats["nodes"][node]["thread_pool"][thread_pool]["active"]
            value = 'total:{} / active:{} / queue:{}'.format(threads,active,queue)
            column_data.append(value)
        df[name] = column_data 
    
    styles = [dict(selector = '',props = [("text-align", "center"),('background-color', 'white'),('border-color', 'black'),('border-spacing','2px'),('border','1.5px solid')]),    
            dict(selector= 'th', props= [('font-size', '12px'),('border-style' ,'solid'),('border', '2px solid black'),('border-width', '0.25px'),('height',"25px"),('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'),("font-weight", "normal"), ('vertical-align', 'centre')]),
            dict(selector= "tbody td", props= [("border", "1px solid grey"),('font-size', '12px'),('border-width', '0.25px')])]

    df = df.style.set_table_styles(styles).set_properties(subset=df.columns, **{'width': '100px'}).hide_index()
    dfnew = df.apply(highlight, axis = 1).applymap(red_color,subset = df.columns)
    return dfnew,disk_data

def GetClusterNameAndDateTime(cat_health):
    global folder_name,zip_folder_name
    for cluster in cat_health:
        epoch_time = float(cluster["epoch"])
        cluster_name = cluster["cluster"]
        date_time = str(datetime.datetime.fromtimestamp( epoch_time ))
    date = str(date_time.replace(":","").replace("-","")).split(" ")
    folder_name = 'PSR_'+cluster_name+"_"+date[0]+"T"+date[1]
    zip_folder_name = folder_name
    path = os.getcwd()
    dir = os.path.join(path,folder_name)
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        shutil.rmtree(dir,ignore_errors=True)
        os.makedirs(dir)    
    return cluster_name,date_time     

def CreateLink(description,file_name):
    whole_list = []
    header_list = []
    for row in description.split("\n"):
        if(row!=""):
            row_list = []  
            if("," in row):
                if(len(row.split(",")) > 1):            
                    for value in row.split(","):  
                        if("=" in value):
                            if(len(header_list)<len(row.split(","))):
                                header_list.append(value.split("=")[0])   
                            row_list.append(value.split("=")[1])
            elif("=" in row):
                if(len(header_list)<len(row.split("="))-1):
                    header_list.append(row.split("=")[0])
                row_list.append(row.split("=")[1])
            while("" in row_list) :
                row_list.remove("")
            whole_list.append(row_list)

    dataframe = pandas.DataFrame(whole_list,columns = header_list) 
    pandas.set_option("max_colwidth", 50)

    styles = [dict(selector = '',props = [("text-align", "center"),('background-color', 'white'),('border-color', 'black'),('border-spacing','2px'),('border','1.5px solid')]),    
            dict(selector= 'th', props= [('font-size', '12px'),('border-style' ,'solid'),('border', '2px solid black'),('border-width', '0.25px'),('height',"25px"),('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'),("font-weight", "normal"), ('vertical-align', 'left')]),
            dict(selector= "tbody td", props= [("border", "1px solid grey"),('font-size', '12px'),('border-width', '0.25px')])]

    dfnew = dataframe.style.set_table_styles(styles).hide_index()

    html = '''<html>
                <head>
                    <style>
                    table{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            width:100%;
                         }}
                     th{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            text-align: center;
                            background-color : #0066CC;
                            color:white;
                            font-size:12px;
                         }}
                     td{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            text-align: center;
                            background-color : white;
                            font-size:12px;
                         }}
                     back_button{{
                            text-align: center;
                            background-color : #0066CC;
                            color:white;
                            font-size:12px;
                        }}
                    </style>
                </head>
                <body>
                    <form>
                        <input type="button" id = "back_button" value="Back" onclick="history.go(-1)">
                    </form>
                    <br></br>{}
                </body>
            </html>'''.format(dfnew.render())
    path = os.getcwd()
    file_path = os.path.join(path,folder_name)
    with open(os.path.join(file_path, file_name), 'w') as fp:
        fp.write(html)
        fp.close()
    file_path = os.path.join(file_path,file_name)
    description = '<a href="{}">{}</a>'.format(file_path,file_name.split(".")[0])
    return description  
  
    
#   *********************************************************** CHECKS *******************************************************************

# ************************ OPERATING SYSTEM CHECKS ***************************

def CheckCPULoadAverage(dataframe,node_stats,nodes,CPULoadAverage):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    warning_cases = []
    fail_cases = []
    nodes_corecount = []
    for node1 in node_stats['nodes']:
        loadaverage_15min = node_stats['nodes'][node1]['os']['cpu']['load_average']["15m"]
        for node2 in nodes["nodes"]:
            core_count = 0
            if(node_stats['nodes'][node1]['name'] == nodes["nodes"][node2]['name']):
                for thread_pool in nodes["nodes"][node2]["thread_pool"]:
                    for parameter in nodes["nodes"][node2]["thread_pool"][thread_pool]:
                        if(parameter == "core"):
                            core_value = nodes["nodes"][node2]["thread_pool"][thread_pool][parameter]
                            core_count += core_value
                nodes_corecount.append([nodes["nodes"][node2]["name"],core_count,loadaverage_15min])

    for node in nodes_corecount:
        if(float(node[2]) >= 0.7*float(node[1]) and float(node[2]) <= 0.8*float(node[1])): 
            warning_cases.append(node)
        elif(float(node[2]) > 0.8*float(node[1])):
            fail_cases.append(node)
    if(len(warning_cases)==0 and len(fail_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'There is no additional CPU load. for precise usages per node, check the overview section.'
    else:
        description = '<b>Issue:</b>\nLoad of {} nodes exceeded the CPU\'s capacity, indicating one or more thread pools of nodes are running low.\n'.format(len(warning_cases)+len(fail_cases))
        description1 = " "
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = 'WARNING'
            total_warning_checks+=1
        elif(len(fail_cases)>0):
            result = "FAIL"
            total_fail_checks+=1
        if(len(warning_cases)!=0):
            for node in warning_cases:
                description1 += 'Node = {}, Current core count = {}, 15 min Load average = {}, Expected load average = {}\n'.format(node[0],node[1],node[2],round(0.7*node[1],2))
        if(len(fail_cases)!=0):
            for node in fail_cases:
                description1 += 'Node = {}, Current core count = {}, 15 min Load average = {}, Expected load average = {}\n'.format(node[0],node[1],node[2],round(0.7*node[1],2))
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'cpu_15minloadaverage.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n\n<b>Nodes:</b>\n'+description1
        suggestion =  '\n<b>Suggestion:</b>\n1. Make sure ES process is what consuming all the CPU.\n2. Find high CPU consuming slow queries and try to tune them(check slow log report for the list of queries).\n3. Scale out more ES nodes.\n'
        description += suggestion
    list_row = [CPULoadAverage,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckMemoryUsagePercent(dataframe,node_stats,MemoryUsagePercent):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

    node_count = 0
    warning_cases = []
    fail_cases = []
    for node_key,node_value in node_stats['nodes'].items():
        value = node_stats['nodes'][node_key]['os']['mem']['used_percent']
        node_count+=1
        if(value > 90):
            fail_cases.append([node_stats['nodes'][node_key]['name'],value])
        elif(value > 85 and value <= 90):
            warning_cases.append([node_stats['nodes'][node_key]['name'],value])

    if len(fail_cases)==0 and len(warning_cases) == 0:
        result = 'PASS'
        total_pass_checks+=1
        description = 'Total Nodes = {}. Memory consumption of all nodes is < 85%. Check overall stats section for detailed usage.'.format(node_count)
    else:
        if len(fail_cases)>=0 :
            result = 'FAIL'
            total_fail_checks+=1
        elif len(warning_cases)>=0 and len(fail_cases)==0:
            result = 'WARNING'
            total_warning_checks+=1
        cases = warning_cases+fail_cases
        cases = sorted(cases,key=lambda l:l[1],reverse=True)
        description = "<b>Issue:</b>\nTotal nodes = {}. Memory consumption of {} nodes is > 85%.\n".format(node_count, len(cases)) 
        description1 = " "
        for node in cases:
            description1 += "Node = {}, Memoryused = {}%\n".format(node[0],node[1])        
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'memory_used.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n\n<b>Nodes:</b>\n'+description1
        suggestion =  '\n<b>Suggestion: </b>\nLogin to the specific ES VM and use top command to check below items.\n1. Is ES process itself is consuming all the memory or any other process.\n2. If ES process memory is high and growing then it can be a native memory leak. Needs further debugging.'
        description += suggestion
    list_row = [MemoryUsagePercent,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    
def CheckDiskUsageLimits(dataframe,cat_allocation,Disk_Limits):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
    
    fail_cases = []
    warning_cases = []
    for node in cat_allocation:   
        if((node['disk.percent']) != None):
            if(int(node['disk.percent']) > 90):
                fail_cases.append([node["node"], node['disk.percent']])
            elif(int(node['disk.percent']) >= 80 and int(node['disk.percent']) <= 90):
                warning_cases.append([node["node"], node['disk.percent']])
    if(len(fail_cases)==0 and len(warning_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = "All nodes have < 80% disk usage."
    else:
        description = "<b>Issue:</b>\n{} nodes have > 80% disk usage.\n".format(len(fail_cases)+len(warning_cases))
        description1 = " "
        cases = warning_cases+fail_cases
        cases = sorted(cases,key=lambda l:l[1],reverse=True)
        if(len(fail_cases)==0 and len(warning_cases)>0):
            result = 'WARNING'
            total_warning_checks+=1
        else:
            result = 'FAIL'
            total_fail_checks+=1
        for node in cases:
            description1 += 'Node = {0}, Disk percent = {1}%\n'.format(node[0],node[1])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'disk_percent.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion =  '\n<b>Suggestion: </b>\n1. Find any unwanted indices and delete them.\n2. Scale the disk if current disk allocation < 2tb.\n3. If we have crossed 2tb per node limit then scale out more nodes.\n'
        description += suggestion
    list_row = [Disk_Limits,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe 

def CheckDiskUsageDistribution(dataframe,cat_allocation,Disk_Usage):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

    nodes_disk_used = []
    for node in cat_allocation:
        if((node['disk.used'])!=None):
            nodes_disk_used.append([node['node'],parse_bytes(node['disk.used'])])
    derived_standard_deviation = statistics.stdev([row[1]/(1024**3) for row in nodes_disk_used])    
    if(derived_standard_deviation <= acceptable_standard_deviation):
        result = 'PASS'
        total_pass_checks+=1
        description = "All nodes have good distirbution of disk usage. Check Overall stats section for detailed usage info."
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description1 = " "
        description = "<b>Issue:</b>\n Some nodes don't have a good distribution of disk usage. Observed standard deviation of {}. \nCheck overall stats section to find detailed per node usage.\n".format(derived_standard_deviation)
        for node in nodes_disk_used:          
            description1 += "Node = {}, Disk used = {}GB \n".format(node[0],round(node[1]/(1024**3)))
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'disk_usage_distribution.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n <b>Suggestion:</b>\nCheck if all the shards distribution is proper.\n'
        description += suggestion
    list_row = [Disk_Usage,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    

def CheckSwap(dataframe,nodes_stats,SwapDisabled):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
    swap_enabled_nodes = []    
    for node in nodes_stats["nodes"]:
        swap_space = nodes_stats["nodes"][node]["os"]["swap"]["total_in_bytes"] 
        if(swap_space != 0):
            swap_enabled_nodes.append([nodes_stats["nodes"][node]["name"],swap_space])   
    if(len(swap_enabled_nodes)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Swap disabled for all nodes.'
    else:      
        result = 'FAIL'
        total_fail_checks+=1
        description = '<b>Issue:</b>\nSwap enabled for below nodes.\n'.format(len(swap_enabled_nodes))
        description1 = " "
        for node in swap_enabled_nodes:
            description1 += 'Node = {}, Swap space = {}\n'.format(node[0],node[1])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'swap.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion =  '\n<b>Suggestion:</b>\nSwap usage impacts the performance. Disable the swap using "/sbin/sysctl -w vm.swappiness=0" \n'
        description += suggestion
    list_row = [SwapDisabled,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
 
 
# ************************ ELASTIC SEARCH USAGE STATS CHECKS ***************************

def CheckStatus(dataframe, cluster_health,Health_check):
  global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
  
  status = cluster_health['status']
  
  if(status=="green"):
      result = 'PASS'
      total_pass_checks+=1
      description = 'cluster health is {}.\nCluster is in healthy status.'.format(status)
  elif(status=="red"):
      result = 'FAIL'
      total_fail_checks+=1
      description = '<b>Issue:</b>\nCluster health is {}. \nFew nodes are down, inactive primary shards.\n'.format(status)
      suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/health to monitor the status.'
      description += suggestion
  elif(status=="yellow"):
      result = 'WARNING'
      total_warning_checks+=1
      description = '<b>Issue:</b>\nCluster health is {}. \nInactive primary or replica shards. Cluster still in usable state. May take some time to recover to green.\n'.format(status)
      suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/health to monitor the status.\n'
      description += suggestion
  else:
      result = 'UNKNOWN'
      description = '<b>Issue:</b>\nCluster health is {}. Not able to fetch the status. Diagnostics tool error.\n'
      suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/health to get more info.'
      description += suggestion
  list_row = [Health_check,result,description]
  dataframe.loc[len(dataframe)] = list_row
  return dataframe

def CheckIsMasterRequired(dataframe,cat_nodes,Master_required):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    datanodes_count = 0
    masternodes_count = 0
    for node in cat_nodes:
        if(node["master"]=="*"):
            masternodes_count += 1
        if("d" in node["node.role"]):
            datanodes_count += 1
    if(datanodes_count < 10):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Current data nodes = {} < 10. Dedicated masters are not required.'.format(datanodes_count)
    else:
        if(datanodes_count > 12):
            result = 'FAIL'
            total_fail_checks+=1
        elif(datanodes_count >=10 and datanodes_count<=12):
            result = 'WARNING'
            total_warning_checks+=1
        
        if(masternodes_count == 3):
            description = 'Current data nodes = {} > 10. Master nodes = {}.'.format(datanodes_count,masternodes_count)
        elif(masternodes_count>3):
            description = 'Current data nodes = {} > 10. Master nodes = {} > 3. Dedicated masters are not required.\n'.format(datanodes_count,masternodes_count)
        else:
            description = '<b>Issue:</b>\nCurrent data nodes = {} > 10. Master nodes = {} < 3.\n'.format(datanodes_count,masternodes_count)
            suggestion = '\n<b>Suggestion:</b>\nTo keep the cluster active and stable, atleast 3 dedicated masters are recommended if we have > 10 data nodes.\n'
            description += suggestion
    list_row = [Master_required,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe

def CheckRelo(dataframe,cluster_health,Relocation_count):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    relo_count = cluster_health['relocating_shards']
    
    if(relo_count == 0):
        result = 'PASS'
        total_pass_checks+=1
        description = "There are no shards for relocation."
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '<b>Issue:</b>\nRelocation count = {}\nFew nodes are restarted / down which may cause shards relocation.\n'.format(relo_count)
        suggestion = '\n<b>Suggestion:</b>\nWait for cluster to stabilise.'
        description += suggestion
    list_row = [Relocation_count,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    
def CheckActiveShardsPercentage(dataframe,cluster_health,ActiveShardsPercentage):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    active_shards_percent = cluster_health['active_shards_percent_as_number']
    description = 'Current active shards percentage is {}% \n'.format(active_shards_percent)
    if(active_shards_percent==100.0):
        result = 'PASS'
        total_pass_checks+=1
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '<b>Issue:</b>\nCurrent active shards percentage is {}% \nShards might be in initialisation state, relocating, or failed for placement.\n'.format(active_shards_percent)
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/allocation/explain api to find the reason.'
        description += suggestion
    list_row = [ActiveShardsPercentage,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    
def CheckShardsDistribution(dataframe,cat_allocation,ShardsDistribution):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    shards_nodes = []
    for node in cat_allocation:
        if(node['node'] != "UNASSIGNED"):
            shards_nodes.append([node['node'],node['shards']])
    derived_standard_deviation = statistics.stdev([int(node[1]) for node in shards_nodes])    
    if(derived_standard_deviation <= acceptable_standard_deviation):
        result = 'PASS'
        total_pass_checks+=1
        description = "There is a healthy distribution of shards on all nodes."
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = "<b>Issue:</b>\nBelow nodes don't have a good distribution of shards. Observed standard deviation = {}\n".format(derived_standard_deviation)
        description1 = " "
        for i in shards_nodes:
            description1 += "Node = {}, Shards = {} \n".format(i[0],i[1])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'shard_distribution.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Check if shard rebalance is enabled and all nodes are in healthy state.\n2. Check for disk distribution also.\n'
        description += suggestion
    list_row = [ShardsDistribution,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe    

def CheckTotalShardsPerNode(dataframe,nodes_stats,cat_allocation,TotalShardsPerNode):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    heapsize_nodes = []
    total_shards = 0
    total_heap = 0
    total_nodes = nodes_stats["_nodes"]["total"]
    for node1 in nodes_stats["nodes"]:
        for node2 in cat_allocation:
            if(nodes_stats["nodes"][node1]['name'] == node2['node']):
                heap_size_in_bytes = nodes_stats["nodes"][node1]["jvm"]["mem"]["heap_max_in_bytes"]   
                total_heap += heap_size_in_bytes
                total_shards += int(node2['shards'])
                if(float(node2['shards']) >= 25*(heap_size_in_bytes)/(1024**3)):
                    heapsize_nodes.append([nodes_stats["nodes"][node1]['name'],(heap_size_in_bytes)/(1024**3),node2['shards'],25*(heap_size_in_bytes)/(1024**3)])
    if(len(heapsize_nodes)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Total shards placed on each node didn\'t exceed the calculation threshold.'
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '<b>Issue:</b>\nTotal shards placed on below nodes exceeded the calculation threshold.\n'.format(len(heapsize_nodes))
        description1 = " "
        for node in heapsize_nodes :
            description1 += 'Node = {}, Shards = {}, Heap size = {}GB, Expected shards = {} \n'.format(node[0],node[2],round(node[1],3),round(node[3]))
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'total_shards_per_node.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Find all empty or unused indices and delete them.\n2. If this is a test environment we can increase the limit from 25 shards per gb memory to 40 shards per gb memory.\n3. If this is a production env, scale out more nodes.\n'
        description += suggestion
    list_row = [TotalShardsPerNode,result,description]
    dataframe.loc[len(dataframe)] = list_row
    total_heap = (heap_size_in_bytes)/(1024**3)
    return dataframe,total_shards,total_nodes,total_heap

def CheckShardsPerIndex(dataframe,cat_indices,Shards_Per_Index):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    warning_cases = []
    fail_cases = []
    for node in cat_indices:
            if(node['status']!="close"):
                IndexName=node['index']
                IndexSizeinGB=round((int(node['pri.store.size'])/(1024**3)),0)
                IndexShardsCount=int(node['pri'])

                Warning_expected_Shards=round(int(node['pri.store.size'])/(35*(1024**3)),0)
                Error_expected_Shards=round(int(node['pri.store.size'])/(50*(1024**3)),0)

                #print(IndexName," ",IndexSizeinGB," ",IndexShardsCount," ",Warning_expected_Shards," ",Error_expected_Shards)
                
                if(IndexShardsCount<Error_expected_Shards):
                    fail_cases.append([IndexName,IndexSizeinGB,IndexShardsCount,Error_expected_Shards]) 
                elif(IndexShardsCount>Error_expected_Shards and IndexShardsCount<Warning_expected_Shards):
                    warning_cases.append([IndexName,IndexSizeinGB,IndexShardsCount,Warning_expected_Shards])

    if(len(warning_cases)==0 and len(fail_cases)==0):
            result = 'PASS'
            total_pass_checks+=1
            description = 'All nodes have good shard count distribution.'
    else:
        description = "<b>Issue:</b>\nFound {} indices with wrong shards count. Please update the shards for better performance. \nIdeally each shard can be up to 35-50GB.\n".format(len(warning_cases)+len(fail_cases))
        description1 = " "
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = 'WARNING'
            total_warning_checks+=1           
            for node in warning_cases:
                description1 += 'Index = {}, Index Size in GB = {}, Current Shards = {}, Expected shards = {} \n'.format(node[0],node[1],node[2],node[3])
        elif(len(fail_cases)>0):
            result = 'FAIL'
            total_fail_checks+=1
            if(len(warning_cases)!=0):
                for node in warning_cases:
                    description1 += 'Index = {}, Index Size in GB  = {}, Current Shards = {}, Expected shards = {} \n'.format(node[0],node[1],node[2],node[3])
            for node in fail_cases:
                description1 += 'Index = {}, Index Size in GB = {}, Current Shards = {}, Expected shards = {}\n'.format(node[0],node[1],node[2],node[3])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'shards_per_index.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Going more than 50GB per shard will impact the performance.\n 2. Increase the shards using _reindex api which also helps in resolving fragmentation issues.\n'
        description += suggestion
    list_row = [Shards_Per_Index,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe 

def CheckUnassignedShards(dataframe,cat_shards,UnassignedShards):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

    unassigned_shards = []
    for shard in cat_shards:
        if(shard["state"]=='UNASSIGNED'):
            if("unassigned.reason" in shard):
                unassigned_shards.append([shard["index"],shard["unassigned.reason"]])
            else:
                unassigned_shards.append([shard["index"],"none"])
    if(len(unassigned_shards)==0):
        result = 'PASS'
        total_pass_checks += 1
        description = 'All shards are assigned.'
    else:
        result = 'FAIL'
        total_fail_checks += 1
        description = '<b>Issue:</b>\nBelow shards are not assigned.\n'
        description1 = " "
        for shard in unassigned_shards:
            description1 += 'Index = {}, Reason = {}\n'.format(shard[0],shard[1])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'unassigned_shards.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cat/shards?h=index,shard,prirep,state,unassigned.reason,ip | grep UNASSIGNED for more info.\n'
        description += suggestion
    list_row = [UnassignedShards,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    
def CheckClusterPendingTasks(dataframe,cluster_pending,ClusterPendingTasks):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    less_time_consuming_tasks_count = 0    
    if cluster_pending["tasks"] == []:
        result = 'PASS'
        total_pass_checks+=1
        description = "There are no cluster pending tasks."
    else:
        warning_tasks = []
        fail_tasks = []
        for task in cluster_pending['tasks']:
            value = task['time_in_queue_millis']
            if(value >= 5*10E3 and value<=10*10E3):
                warning_tasks.append([task["source"],task['priority'],value])
            elif(value>10*10E3):
                fail_tasks.append([task["source"],task['priority'],value])
            elif(value < 5*10E3):
                less_time_consuming_tasks_count += 1
        if(len(warning_tasks)==0 and len(fail_tasks)==0):
            result = 'PASS'
            total_pass_checks+=1
            if(less_time_consuming_tasks_count == 0):
                description = "There are no cluster pending tasks."
            else:
                description = 'There are {} cluster pending tasks with a duration of < 5secs.'.format(less_time_consuming_tasks_count)
        else:
            description1 =  " "
            if(len(warning_tasks)>0 and len(fail_tasks)==0):
                result = 'WARNING'
                total_warning_checks+=1
                description = '<b>Issue:</b>\nThe following are the cluster pending tasks that have been identified with a duration of 5-10secs.\n'
                for task in warning_tasks:
                    description1 += 'Source = {0}, Priority = {1}, Runningtime = {2} sec(s) \n'.format(task[0],task[1],round(task[2]/10e3,2))
            elif(len(fail_tasks)>0):
                result = 'FAIL'
                total_fail_checks+=1
                description = '<b>Issue:</b>\nThe following are the cluster pending tasks that have been identified.\n'
                if len(warning_tasks)!=0:
                    for task in warning_tasks:
                        description1 += 'Source = {0}, Priority = {1}, Runningtime = {2} sec(s) \n'.format(task[0],task[1],round(task[2]/10e3,2))
                for task in fail_tasks:
                    description1 += 'Source = {0}, Priority = {1}, Runningtime = {2} sec(s) \n'.format(task[0],task[1],round(task[2]/10e3,2))
            if(description1.count('\n') > 10):
                description_link = CreateLink(description1,'cluster_pending_tasks.html')
                description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
            else:
                description += '\n<b>Tasks:</b>\n'+description1
            suggestion = '\n<b>Suggestion:</b>\nGet the high time consuming tasks and need to debug based on the tasks.\n'
            description += suggestion
    list_row = [ClusterPendingTasks,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe

def CheckTasks(dataframe,_tasks,HighTimeConsumingTasks):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
    
    if _tasks == []:
        result = 'PASS'
        total_pass_checks+=1
        description = "There are no high time consuming tasks."
    else:
        less_time_consuming_tasks_count = 0
        warning_tasks = []
        fail_tasks = []
        
        for node in _tasks['nodes']:
            for task in _tasks['nodes'][node]['tasks']:
                running_time = _tasks['nodes'][node]['tasks'][task]['running_time_in_nanos']
                if(running_time >= 5*10E9 and running_time<=10*10E9):
                    warning_tasks.append([task,_tasks['nodes'][node]['tasks'][task]['node'],running_time,_tasks['nodes'][node]['tasks'][task]['action']])
                elif(running_time>10*10E9):
                    fail_tasks.append([task,_tasks['nodes'][node]['tasks'][task]['node'],running_time,_tasks['nodes'][node]['tasks'][task]['action']])
                elif(running_time < 5*10E9):
                    less_time_consuming_tasks_count += 1
        if(len(warning_tasks)==0 and len(fail_tasks)==0):
            result = 'PASS'
            total_pass_checks+=1
            if(less_time_consuming_tasks_count == 0):
                description = "There are no high time consuming tasks."
            else:
                description = 'There are {} high time consuming tasks with a duration of < 5secs.'.format(less_time_consuming_tasks_count)
        else:
            description1 = " "
            if(len(warning_tasks)>0 and len(fail_tasks)==0):
                result = 'WARNING'
                total_warning_checks+=1
                description = '<b>Issue:</b>\nThe following are the high time consuming tasks that have been identified with a duration of 5-10secs.\n'
                for task in warning_tasks:
                    description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3}\n'.format(task[0],task[1],round(task[2]/10e9,2),task[3])
            else:
                result = 'FAIL'
                total_fail_checks+=1
                description = '<b>Issue:</b>\nThe following are the high time consuming tasks that have been identified.\n'
                if(len(warning_tasks)==0 and len(fail_tasks)>0):    
                    for task in fail_tasks:
                        description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3} \n'.format(task[0],task[1],round(task[2]/10e9,2,),task[3])
                elif(len(warning_tasks)>0 and len(fail_tasks)>0):
                    for task in warning_tasks:
                        description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3} \n'.format(task[0],task[1],round(task[2]/10e9,2),task[3])
                    for task in fail_tasks:
                        description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3} \n'.format(task[0],task[1],round(task[2]/10e9,2),task[3])
            if(description1.count('\n') > 10):
                description_link = CreateLink(description1,'tasks.html')
                description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
            else:
                description += '\n<b>Tasks:</b>\n'+description1
            suggestion = '\n<b>Suggestion:</b>\nGet the high time consuming tasks and need to debug based on the tasks.\n'
            description += suggestion
    list_row = [HighTimeConsumingTasks,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    

def CheckHeapSizeDataNodes(dataframe,nodes_stats,Heapsize_datanodes):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks        
    warning_cases = []
    fail_cases = []
    for node in nodes_stats["nodes"]:  
        heap_size_in_bytes = (nodes_stats["nodes"][node]["jvm"]["mem"]["heap_max_in_bytes"])
        if((heap_size_in_bytes)>20*(1024**3) and (heap_size_in_bytes)<30*(1024**3)):
            warning_cases.append([nodes_stats["nodes"][node]['name'],heap_size_in_bytes/(1024**3)])
        elif(heap_size_in_bytes <=20*(1024**3)):
            fail_cases.append([nodes_stats["nodes"][node]['name'],heap_size_in_bytes/(1024**3)])  
    if(len(warning_cases)==0 and len(fail_cases)==0):
            result = 'PASS'
            total_pass_checks+=1
            description = 'All nodes have required heap size.'
    else:
        description = "<b>Issue:</b>\nFound {} indices with heap size < 30GB.\n".format(len(warning_cases)+len(fail_cases))
        description1 = " "
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = 'WARNING'
            total_warning_checks+=1           
            for node in warning_cases:
                description1 += 'Node = {}, Heap size = {:0.3f}GB\n'.format(node[0],node[1])
        elif(len(fail_cases)>0):
            result = 'FAIL'
            total_fail_checks+=1
            if(len(warning_cases)!=0):
                for node in warning_cases:
                    description1 += 'Node = {}, Heap size = {:0.3f}GB\n'.format(node[0],node[1])
            for node in fail_cases:
                description1 += 'Node = {}, Heap size = {:0.3f}GB\n'.format(node[0],node[1])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'data_nodes_heap.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nAs per ES recommendation, 30GB heap is optimal for performance.\n'
        description += suggestion
    list_row = [Heapsize_datanodes,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
  
def CheckHeapSizeMasterNodes(dataframe,nodes_stats,cat_nodes,Heapsize_masternodes):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

    warning_cases = []
    fail_cases = []
    masternodes_count = 0           
    for node in cat_nodes:
        if(node["master"]=="*"):
            masternodes_count+=1
            for node1 in nodes_stats["nodes"]:
                if(nodes_stats["nodes"][node1]["name"]==node["name"]):
                    heap_size_in_bytes = nodes_stats["nodes"][node1]["jvm"]["mem"]["heap_max_in_bytes"]
                    if((heap_size_in_bytes)>=4*(1024**3) and (heap_size_in_bytes)<6*(1024**3)):
                        warning_cases.append([nodes_stats["nodes"][node1]["name"],round(heap_size_in_bytes/(1024**3),3)])
                    elif(heap_size_in_bytes <4*(1024**3)):
                        fail_cases.append([nodes_stats["nodes"][node1]["name"],round(heap_size_in_bytes/(1024**3),3)])  
    if(len(warning_cases)==0 and len(fail_cases)==0):
            result = 'PASS'
            total_pass_checks+=1
            if(masternodes_count == 0):
                description = 'There are no master nodes found.'
            else:
                description = 'Number of master nodes = {}. All master nodes have required heap size.'.format(masternodes_count)
    else:
        description = "<b>Issue:</b>\nTotal master nodes  = {0}.\nDedicated master node should have around 8GB to 16GB of physical memory with a heap size of 75% of the physical memory(6GB).\nfound below master nodes with heap size < 6GB.\n".format(masternodes_count,len(warning_cases)+len(fail_cases))
        description1 = " "
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = 'WARNING'
            total_warning_checks+=1           
            for node in warning_cases:
                description1 += 'Node = {}, Heap size = {}GB\n'.format(node[0],node[1])
        elif(len(fail_cases)>0):
            result = 'FAIL'
            total_fail_checks+=1
            if(len(warning_cases)!=0):
                for node in warning_cases:
                    description1 += 'Node = {}, Heap size = {}GB\n'.format(node[0],node[1])
            for node in fail_cases:
                description1 += 'Node = {}, Heap size = {}GB\n'.format(node[0],node[1])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'master_nodes_heap.html')
            description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nMake sure dedicated master heap setting is atleast 4-6GB\n'
        description += suggestion
    list_row = [Heapsize_masternodes,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
    
def CheckTotalMemoryDataNodes(dataframe,nodes_stats,TotalMemoryofMasterNodes):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    fail_cases = []
    for node in nodes_stats["nodes"]:
        heap_size_in_bytes = nodes_stats["nodes"][node]["jvm"]["mem"]["heap_max_in_bytes"]
        memory_size_in_bytes = nodes_stats["nodes"][node]["os"]["mem"]["total_in_bytes"] 
        expected_size_in_bytes = 2*heap_size_in_bytes+2*(1024**3)
        if(memory_size_in_bytes < expected_size_in_bytes):
            fail_cases.append([nodes_stats["nodes"][node]['name'],round(heap_size_in_bytes/(1024**3),3),round(memory_size_in_bytes/(1024**3),3),expected_size_in_bytes])  
        
    if(len(fail_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Nodes are configured properly.'
    else:
        description = "<b>Issue:</b>\nBasically, total RAM of a data node should be twice the heap size.\nThe nodes listed below have wrong configuration.\n"
        description1 = " "
        result = 'FAIL'
        total_fail_checks+=1  
        for node in fail_cases:
            description1 += 'Node = {}, Heap size = {}GB, Current memory = {}GB, Expected memory = {}GB\n'.format(node[0],node[1],node[2],round(node[3]/(1024**3)))
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'total_memory_data_nodes.html')
            description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nAs per ES documentation, ES needs atleast double the heap space as memory. Scale up the memory.\n'
        description += suggestion
    list_row = [TotalMemoryofMasterNodes,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
   
def CheckThreadpoolUsage(dataframe,node_stats,ThreadpoolUsage):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    nodes_count = []
    warning_cases = []
    fail_cases = []
    for node in node_stats['nodes']:
        threads_count = node_stats['nodes'][node]['jvm']['threads']['count']
        active_threads_count = 0
        queue = 0
        for thread_pool in node_stats["nodes"][node]["thread_pool"]:
            for parameter in node_stats["nodes"][node]["thread_pool"][thread_pool]:
                if(parameter == "active"):
                    active_count = node_stats["nodes"][node]["thread_pool"][thread_pool][parameter]
                    active_threads_count += active_count
                elif(parameter == "queue"):
                    queue += node_stats["nodes"][node]["thread_pool"][thread_pool][parameter]
        nodes_count.append([node_stats["nodes"][node]["name"],active_threads_count,threads_count,queue])

    for node in nodes_count:
        if(float(node[1]) >= 0.7*float(node[2]) and float(node[1]) <= 0.8*float(node[2])):
            warning_cases.append(node)
        elif(float(node[1]) > 0.8*float(node[2]) or node[3]>node[2]):
            fail_cases.append(node)

    if(len(warning_cases)==0 and len(fail_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Workload of the thread pool is maintained.'
    else:
        description = '<b>Issue:</b>\nThere are {} busy thread pools found.\n'.format(len(warning_cases)+len(fail_cases))
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = "WARNING"
            total_warning_checks+=1
        elif(len(fail_cases)>0):
            result = "FAIL"
            total_fail_checks+=1
        description1 = ''
        if(len(warning_cases)!=0):
            for node in warning_cases:
                description1 += 'Node = {}, Active threads = {}, Total threads = {}, Queue = {}\n'.format(node[0],node[1],node[2],node[3])
        if(len(fail_cases)!=0):
            for node in fail_cases:
                description1 += 'Node = {}, Active threads = {}, Total threads = {}, Queue = {}\n'.format(node[0],node[1],node[2],node[3])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'thread_pool.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Find the busy pool and fix the issue accordingly.\n2. May need to scale out more nodes to support the load.\n'
        description += suggestion
    list_row = [ThreadpoolUsage,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe

def CheckSegmentFragmentation(dataframe,cat_indices,SegmentFragmentationLevel):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    warning_cases = []
    fail_cases = []
    for index in cat_indices:
        if(index['status']!="close"):
            total_docs = int(index["docs.count"])
            deleted_docs = int(index["docs.deleted"])
            if(total_docs!=0):
                level = (deleted_docs/(total_docs+deleted_docs))*100
                if(level >= 40 and level <= 50):
                    warning_cases.append([index["index"],deleted_docs,total_docs,level])
                elif(level>50):
                    fail_cases.append([index["index"],deleted_docs,total_docs,level])
    if(len(warning_cases)==0 and len(fail_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Segment fragmentation level is maintained for all indices.'
    else:
        cases = warning_cases+fail_cases
        description = '<b>Issue:</b>\nThere are {} indices with high segment fragmentation level. \n'.format(len(cases))
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = "WARNING"
            total_warning_checks+=1
        elif(len(fail_cases)>0):
            result = "FAIL"
            total_fail_checks+=1
        description1 = ' '
        cases = sorted(cases,key=lambda l:l[3],reverse=True)
        for node in cases:
            description1 += 'Index = {}, Deleted docs = {}, Total_docs = {}, Percent = {}%\n'.format(node[0],node[1],node[2],round(node[3]))
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'segments_fragmentation.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nUse _reindex api to create a fresh index.\n'
        description += suggestion
    list_row = [SegmentFragmentationLevel,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe            

def CheckEmptyIndices(dataframe,cat_indices,EmptyIndices):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
    
    empty_index = []
    for index in cat_indices:
        if(index['docs.count']=='0'):
            empty_index.append(index['index'])
    if(len(empty_index)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'There are no empty indices in the cluster.'
    else:
        description = '<b>Issue:</b>\nThere are {} empty indices found.\n'.format(len(empty_index))
        if(len(empty_index)<=5):
            result = 'WARNING'
            total_warning_checks+=1           
        elif(len(empty_index)>5):
            result = 'FAIL'
            total_fail_checks+=1
        empty_index = sorted(empty_index)
        description1 = " "    
        for index in empty_index:
            description1 += 'Index = {}\n'.format(index)
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'empty_index.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Delete all the unwanted empty indices.\n2. Every index takes few resources and also we hit max shards limit and we end up in unwanted nodes scale out.\n'
        description += suggestion
    list_row = [EmptyIndices,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
 
def CheckOpenFileDescriptors(dataframe,nodes_stats,OpenFileDescriptors):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

    warning_cases = []
    fail_cases = []    
    for node in nodes_stats["nodes"]:
        open_file_descriptors = nodes_stats["nodes"][node]["process"]["open_file_descriptors"]
        max_file_descriptors = nodes_stats["nodes"][node]["process"]["max_file_descriptors"]    
        percentage = (open_file_descriptors/max_file_descriptors)*100
        if(percentage >=30 and percentage <= 50):
            warning_cases.append([nodes_stats["nodes"][node]["name"],open_file_descriptors,max_file_descriptors,round(percentage)])
        elif(percentage > 50):
            fail_cases.append([nodes_stats["nodes"][node]["name"],open_file_descriptors,max_file_descriptors,round(percentage)])
    if(len(warning_cases)==0 and len(fail_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Open file decriptors are maintained.'
    else:
        description = '<b>Issue:</b>\nOpen file descriptors of some nodes are exceeding their limit.\n'
        description1 = " "
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = "WARNING"
            total_warning_checks+=1
        elif(len(fail_cases)>0):
            result = "FAIL"
            total_fail_checks+=1
        if(len(warning_cases)!=0):
            for node in warning_cases:
                description1 += 'Node = {}, Open file descriptors = {}, Max file descriptors = {}, Percentage = {}%\n'.format(node[0],node[1],node[2],node[3])
        if(len(fail_cases)!=0):
            for node in fail_cases:
                description1 += 'Node = {}, Open file descriptors = {}, Max file descriptors = {}, Percentage = {}%\n'.format(node[0],node[1],node[2],node[3])
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'open_file_descriptors.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nUse lsof command to find all the open file descriptors to find the reason for high consumption.\n'
        description += suggestion
    list_row = [OpenFileDescriptors,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe   

def CheckCircuitBreakers(dataframe,nodes_stats,CircuitBreakers):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

    warning_cases = []
    fail_cases = []    
    for node in nodes_stats['nodes']:
        for breaker in nodes_stats["nodes"][node]["breakers"]:
            limit_size_in_bytes = nodes_stats["nodes"][node]["breakers"][breaker]["limit_size_in_bytes"]
            estimated_size_in_bytes = nodes_stats["nodes"][node]["breakers"][breaker]["estimated_size_in_bytes"]
            percentage = (estimated_size_in_bytes/limit_size_in_bytes)*100       
            if(percentage >=70 and percentage <= 80):
                warning_cases.append([nodes_stats["nodes"][node]["name"],breaker,round(estimated_size_in_bytes/(1024**3)),round(limit_size_in_bytes/(1024**3)),percentage])
            elif(percentage > 80):
                fail_cases.append([nodes_stats["nodes"][node]["name"],breaker,round(estimated_size_in_bytes/(1024**3)),round(limit_size_in_bytes/(1024**3)),percentage]) 
    if(len(warning_cases)==0 and len(fail_cases)==0):
        result = 'PASS'
        total_pass_checks+=1
        description = 'Circuit breakers limit is maintained.'
    else:
        description = '<b>Issue:</b>\nCircuit breakers limit of some nodes exceeded the threshold. Node may crash due to an OutOfMemoryError.\n'
        description1 = " "
        if(len(warning_cases)>0 and len(fail_cases)==0):
            result = "WARNING"
            total_warning_checks+=1
        elif(len(fail_cases)>0):
            result = "FAIL"
            total_fail_checks+=1
        if(len(warning_cases)!=0):
            for node in warning_cases:
                description1 += 'Node = {}, Breaker = {}, Estimated memory used = {}GB, Total memory = {}GB, Percentage = {}%\n'.format(node[0],node[1],node[2],node[3],int(node[4]))
        if(len(fail_cases)!=0):
            for node in fail_cases:
                description1 += 'Node = {}, Breaker = {}, Estimated memory used = {}GB, Total memory = {}GB, Percentage = {}%\n'.format(node[0],node[1],node[2],node[3],int(node[4]))
        if(description1.count('\n') > 10):
            description_link = CreateLink(description1,'circuit_breakers.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n <b>Suggestion:</b>\nNeeds increasing of heap size or scale out nodes.\n'
        description += suggestion
    list_row = [CircuitBreakers,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe     

 
# ************************ CLUSTER CONFIGURATION CHECKS *************************** 
   
def CheckClusterConcurrentRebalance(dataframe,cluster_settings,ClusterConcurrentRebalance):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
    
	#TODO. Some times cluster_concurrent_rebalance may be present under defaults.cluster.routing.allocation or under transient.cluster.routing.allocation or under persistent.cluster.routing.allocation. Below logic needs to be changed to check both and pick the value where ever it is present.
    #Fixed TODO

    try:
        cluster_concurrent_rebalance = cluster_settings["transient"]["cluster"]["routing"]["allocation"]["cluster_concurrent_rebalance"]
    except KeyError:
        pass
    try:
        cluster_concurrent_rebalance = cluster_settings["defaults"]["cluster"]["routing"]["allocation"]["cluster_concurrent_rebalance"]
    except KeyError:
        pass
    try:
        cluster_concurrent_rebalance = cluster_settings["persistent"]["cluster"]["routing"]["allocation"]["cluster_concurrent_rebalance"]
    except KeyError:
        pass

    if(cluster_concurrent_rebalance == '2'):
        result = 'PASS'
        total_pass_checks+=1
        description = 'cluster.routing.allocation.cluster_concurrent_rebalance = {}'.format(cluster_concurrent_rebalance)
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\ncluster.routing.allocation.cluster_concurrent_rebalance = {}\nCluster may not be able to rebalance shards as the concurrent rebalance setting is not maintained.\n\n<b>Suggestion:</b>\nTo change the concurrent rebalance settings, use \nPUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.allocation.cluster_concurrent_rebalance": 2
                          }}
                        }}'''.format(cluster_concurrent_rebalance)        
    list_row = [ClusterConcurrentRebalance,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
 
def CheckClusterEnableRebalance(dataframe,cluster_settings,ClusterEnableRebalance):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    cluster_enable_rebalance = cluster_settings["defaults"]["cluster"]["routing"]["rebalance"]["enable"]
    if(cluster_enable_rebalance == "all"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'cluster.routing.rebalance.enable = {}'.format(cluster_enable_rebalance)
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\ncluster.routing.rebalance.enable = {}\nCluster rebalance is disabled.\n\n<b>Suggestion:</b>\nTo enable automatic cluster rebalancing, use 
                        PUT /_cluster/settings?flat_settings=true
                        {{
                            "defaults" : {{
                                "cluster.routing.rebalance.enable": "all",     
                            }}
                        }}'''.format(cluster_enable_rebalance)       
    list_row = [ClusterEnableRebalance,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe

def CheckClusterEnableAllocation(dataframe,cluster_settings,ClusterEnableAllocation):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    cluster_enable_allocation = cluster_settings["defaults"]["cluster"]["routing"]["allocation"]["enable"]
    if(cluster_enable_allocation == "all"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'cluster.routing.allocation.enable = {}'.format(cluster_enable_allocation)
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\ncluster.routing.allocation.enable = {}\nCluster allocation is disabled. \n\n<b>Suggestion:</b>\nTo enable, use \nPUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.allocation.enable": "all"
                          }}
                        }}'''.format(cluster_enable_allocation)         
    list_row = [ClusterEnableAllocation,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe

def CheckAdaptiveReplicaSelection(dataframe,cluster_settings,AdaptiveReplicaSelection):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
    
    if "use_adaptive_replica_selection" in cluster_settings:
        if "use_adaptive_replica_selection" in cluster_settings["transient"]:
            use_adaptive_replica_selection = cluster_settings["transient"]["cluster"]["routing"]["use_adaptive_replica_selection"]
        else:
            use_adaptive_replica_selection = cluster_settings["defaults"]["cluster"]["routing"]["use_adaptive_replica_selection"]
    else:
        use_adaptive_replica_selection="false"
        
    if(use_adaptive_replica_selection == "true"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'cluster.routing.use_adaptive_replica_selection = {}'.format(use_adaptive_replica_selection)
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\ncluster.routing.use_adaptive_replica_selection = {}\nCluster is not using adaptive replica selection.\n\n<b>Suggestion:</b>\nTo enable, use
                        PUT /_cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.use_adaptive_replica_selection": true
                          }}
                        }}'''.format(use_adaptive_replica_selection)         
    list_row = [AdaptiveReplicaSelection,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe 

def CheckUsageOfWildcards(dataframe,cluster_settings,UsageOfWildcards):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
       
    destructive_requires_name = cluster_settings["defaults"]["action"]["destructive_requires_name"]
    if(destructive_requires_name == "true"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'action.destructive_requires_name = {}'.format(destructive_requires_name) 
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\naction.destructive_requires_name = {}\nWildcard usage is enabled.\n\n<b>Suggestion:</b>\nTo disable, use 
                        PUT /_cluster/settings
                        {{
                          "defaults": {{
                            "action.destructive_requires_name":true
                          }}
                        }}'''.format(destructive_requires_name)        
    list_row = [UsageOfWildcards,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe  

def CheckAllowLeadingWildcard(dataframe,cluster_settings,AllowLeadingWildcard):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
       
    allow_leading_wildcard = cluster_settings["defaults"]["indices"]["query"]["query_string"]["allowLeadingWildcard"]
    if(allow_leading_wildcard == "false"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'indices.query.query_string.allowLeadingWildcard = {}'.format(allow_leading_wildcard)
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\nindices.query.query_string.allowLeadingWildcard = {}\nLeading wildcard is enabled. This will create huge impact on overall cluster if someone tries queries with * as leading character.\n\n<b>Suggestion:</b>\nTo disable, use 
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "indices.query.query_string.allowLeadingWildcard": false
                          }}
                        }}'''.format(allow_leading_wildcard)        
    list_row = [AllowLeadingWildcard,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe  

def CheckOpenScrollContext(dataframe,cluster_settings,OpenScrollContext):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    if("search" in cluster_settings["persistent"]):
        max_open_scroll_context = cluster_settings["persistent"]["search"]["max_open_scroll_context"]
        if(int(max_open_scroll_context) == 500):
            result = 'PASS'
            total_pass_checks+=1
            description = 'persistent.search.max_open_scroll_context = {}\nOpen scroll context limit is not reached yet. However, elasticsearch does not allow more than 500 open scroll contexts. '.format(max_open_scroll_context)
        else:
            result = 'FAIL'
            total_fail_checks+=1
            description = '''<b>Issue:</b>\npersistent.search.max_open_scroll_context = {}\nOpen scroll context limit is reached. Elasticsearch does not allow more than 500 open scroll contexts.\n\n<b>Suggestion:</b>\nUse below api to set the limit for Scrolls.\nPUT localhost:9200/_cluster/settings 
            {{
                "persistent" : {{
                    "search.max_open_scroll_context": 500
                }}
            }}'''.format(max_open_scroll_context)
    else:
        max_open_scroll_context = cluster_settings["defaults"]["search"]["max_open_scroll_context"]
        if(int(max_open_scroll_context) == 500):
            result = 'PASS'
            total_pass_checks+=1
            description = 'defaults.search.max_open_scroll_context = {}\nOpen scroll context limit is not reached yet. However, elasticsearch does not allow more than 500 open scroll contexts. '.format(max_open_scroll_context)
        else:
            result = 'FAIL'
            total_fail_checks+=1
            description = '''<b>Issue:</b>\ndefaults.search.max_open_scroll_context = {}\nOpen scroll context limit is reached. Elasticsearch does not allow more than 500 open scroll contexts.\n\n<b>Suggestion:</b>\nUse below api to set the limit for Scrolls.\nPUT localhost:9200/_cluster/settings 
            {{
                "defaults" : {{
                    "search.max_open_scroll_context": 500
                }}
            }}'''.format(max_open_scroll_context)
                         
    list_row = [OpenScrollContext,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe  

def CheckNodeConcurrentRecovery(dataframe,cluster_settings,NodeConcurrentRecovery):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
	#TODO node_concurrent_recoveries may present under transient, defaults, or persistent. Pick the right place to validate it 
    #Fixed TODO
    try:
        node_concurrent_recoveries = cluster_settings["transient"]["cluster"]["routing"]["allocation"]["node_concurrent_recoveries"]
    except KeyError:
        pass
    try:
        node_concurrent_recoveries = cluster_settings["defaults"]["cluster"]["routing"]["allocation"]["node_concurrent_recoveries"]
    except KeyError:
        pass
    try:
        node_concurrent_recoveries = cluster_settings["persistent"]["cluster"]["routing"]["allocation"]["node_concurrent_recoveries"]
    except KeyError:
        pass

    if(node_concurrent_recoveries == "20"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'cluster.routing.allocation.node_concurrent_recoveries = {}'.format(node_concurrent_recoveries)
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\ncluster.routing.allocation.node_concurrent_recoveries = {}\nThe concurrent recoveries setting is set too low. \n\n<b>Suggestion:</b>\nTo change the concurrent recovery settings, use 
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.allocation.node_concurrent_recoveries ": 20
                          }}
                        }}'''.format(node_concurrent_recoveries)        
    list_row = [NodeConcurrentRecovery,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe  

def CheckReadOnlyAllowDelete(dataframe,cluster_settings,ReadOnlyAllowDelete):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
        
    read_only_allow_delete = cluster_settings["defaults"]["cluster"]["blocks"]["read_only_allow_delete"]
    if(read_only_allow_delete == "null" or read_only_allow_delete == "false"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'cluster.blocks.read_only_allow_delete = {}'.format(read_only_allow_delete) 
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\ncluster.blocks.read_only_allow_delete = {}\nRead-only-allow-delete index(s) found. Cluster disk usage might have been hit high water mark causing this.\n\n<b>Suggestion:</b>\nIncrease the disk space and run below api to reset it.
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.blocks.read_only_allow_delete":null
                          }}
                        }}'''.format(read_only_allow_delete)
    list_row = [ReadOnlyAllowDelete,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe

def CheckPainlessRegex(dataframe,cluster_settings,PainlessRegex):
    global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks
       
    painless_regex_enable = cluster_settings["defaults"]["script"]["painless"]["regex"]["enabled"]
    if(painless_regex_enable == "false"):
        result = 'PASS'
        total_pass_checks+=1
        description = 'regex is disabled in Elasticsearch painless scripts.'
    else:
        result = 'FAIL'
        total_fail_checks+=1
        description = '''<b>Issue:</b>\nscript.painless.regex.enabled = {}.\n\n<b>Suggestion:</b>\nTo disable, use 
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "script.painless.regex.enabled":false
                          }}
                        }}'''.format(painless_regex_enable)        
    list_row = [PainlessRegex,result,description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe    



# ******************************************************* SLOW LOG ***********************************************************************

def CheckRepeat(array,row):
    check = 0
    for i in array:
        if(i[0]==row[0]):
            check = 1
            i[3]+=1
            if(type(i[1])==int):
                i[1] = [i[1],row[1]]
            elif(type(i[1])==list):  
                i[1].append(row[1])
            break
            
        else:
            check = 0
    return array,check

def MaskQuery(query):
    if("\"query\":\"" in query):
        array = []    
        for index,value in enumerate(query):
            if query[index:index+(len("\"query\":\""))] == "\"query\":\"":
                string = query[index:query.index("\",",index+(len("\"query\":\"")))]
                array.append(string)
        for i in array:
            query = query.replace(i,"\"query\":\"xxxxx")
    if("\"value\":" in query):
        array = []    
        for index,value in enumerate(query):
            if query[index:index+(len("\"value\":"))] == "\"value\":":
                string = query[index:query.index(",\"boost\":",index+(len("\"value\":")))]
                array.append(string)
        for i in array:
            query = query.replace(i,"\"value\":xxxxx") 
    return query
    
def SlowLog(slow_logs):
    array=[]
    for log in slow_logs:
        if("msg" in log):
            count1,count2,count3,count4 = 0,0,0,0
            for info in log["msg"].split(",",7):
                if("took" in info and count1==0):
                    index_name = info[info.index("[")+1:info.index("]")]
                    count1=1
                if("took_millis" in info and count2==0):
                    time_in_milli = int(info[info.index("[")+1:info.index("]")])
                    count2=1
                if("total_hits" in info and count3==0):
                    total_hits = info[info.index("[")+1:info.index("]")]
                    count3=1
                if("source" in info and count4==0):
                    if("], id[" in info):
                        source = info[info.index("source[")+len("source["):info.index("], id[")]    
                    elif("cluster.uuid" in info):
                        source = info[info.index("source[")+len("source["):info.index(", \"cluster.uuid\":")]
                    count4=1
        elif("message" in log):
            count1,count2,count3,count4 = 0,0,0,0
            for info in log["message"].split(",",7):
                if("took" in info and count1==0):
                    index_name = info[info.index("[")+1:info.index("]")]
                    count1=1
                if("took_millis" in info and count2==0):
                    time_in_milli = int(info[info.index("[")+1:info.index("]")])
                    count2=1
                if("total_hits" in info and count3==0):
                    total_hits = info[info.index("[")+1:info.index("]")]
                    total_hits = total_hits.split(" hits")[0]
                    count3=1
                if("source" in info and count4==0):
                    if("], id[" in info):
                        source = info[info.index("source[")+len("source["):info.index("], id[")]
                    elif("cluster.uuid" in info):
                        source = info[info.index("source[")+len("source["):info.index(", \"cluster.uuid\":")]
                    count4=1
        source = MaskQuery(source)
        row_list = [index_name,time_in_milli,total_hits,1,source] 
        array,check =  CheckRepeat(array,row_list)
        if(check == 0):
            array.append(row_list)
    for row in array:
        if(type(row[1])==list):
            row[1] = round(statistics.mean(row[1]))
    sorted_array = sorted(array,key=lambda l:l[1],reverse=True)
    file_name = 'slow_logs.html'    
    slow_log_dataframe = pandas.DataFrame(sorted_array,columns = ['Index Name','Average Total time in ms','Total hits','Number of repetitions','Actual Query']) 
    pandas.set_option("max_colwidth", 50)
    slow_log_dataframe['Actual Query'] = slow_log_dataframe['Actual Query'].str.wrap(120)
    styles = [dict(selector = '',props = [("text-align", "center"),('background-color', 'white'),('border-color', 'black'),('border-spacing','2px'),('border','1.5px solid')]),    
            dict(selector= 'th', props= [('font-size', '12px'),('border-style' ,'solid'),('border', '2px solid black'),('border-width', '0.25px'),('height',"25px"),('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'),("font-weight", "normal"), ('vertical-align', 'left')]),
            dict(selector= "tbody td", props= [("border", "1px solid grey"),('font-size', '12px'),('border-width', '0.25px')])]

    dfnew = slow_log_dataframe.style.set_table_styles(styles).set_properties(subset=['Actual Query'], **{'text-align': 'left'}).hide_index()
    html = '''<html>
                <head>
                    <style>
                    table{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                         }}
                     th{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            text-align: center;
                            background-color : #0066CC;
                            color:white;
                            font-size:12px;
                         }}
                     td{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            text-align: left;
                            background-color : white;
                            font-size:12px;
                         }}
                    </style>
                </head>
                <body>{}</body>
            </html>'''.format(dfnew.render())
    path = os.getcwd()
    file_path = os.path.join(path,folder_name)
    with open(os.path.join(file_path, file_name), 'w') as fp:
        fp.write(html)
        fp.close()
    return dfnew


# ********************************************************* MAIN PROGRAM *********************************************************************

import elasticsearch
import json
import pandas
import argparse
import requests
import statistics
import jinja2
from dask.utils import parse_bytes,parse_timedelta
import datetime 
import os 
import shutil
import urllib3
import warnings
from zipfile import ZipFile

warnings.filterwarnings("ignore", category=FutureWarning)

global total_pass_checks, total_warning_checks, total_fail_checks, total_unknown_error_checks

acceptable_standard_deviation = 5
total_pass_checks = 0
total_warning_checks = 0
total_fail_checks = 0
total_unknown_error_checks = 0

print("*************")
parser = argparse.ArgumentParser()
parser.add_argument('-StatsZipFile', action = "store",dest = 'StatsZipFile', type=str, help='Add Stats Zip File')
parser.add_argument('-secure',action = "store",dest = 'secure',type = str,help = 'Add Secure/Unsecure')
parser.add_argument('-CanAccessCluster', action = "store", dest = 'CanAccessCluster', type=str, help='CanAccessCluster/Not')
parser.add_argument('-URL', action = "store",dest = 'es_url', type=str, help='Add es_url')
# parser.add_argument('-Username', action = "store",dest = 'username', type=str, help='Add username')
# parser.add_argument('-password', action = "store",dest = 'password', type=str, help='Add password')
# parser.add_argument('-catNodes', action = "store",dest = 'catNodes', type=str, help='Add cat nodes file')
# parser.add_argument('-catShards', action = "store",dest = 'catShards', type=str, help='Add cat shards file')
# parser.add_argument('-catHealth', action = "store",dest = 'catHealth', type=str, help='Add cat health file')
# parser.add_argument('-clusterHealth', action = "store",dest = 'clusterHealth', type=str, help='Add cluster health file')
# parser.add_argument('-catAllocation', action = "store",dest = 'catAllocation', type=str, help='Add cat allocation file')
# parser.add_argument('-catIndices', action = "store",dest = 'catIndices', type=str, help='Add cat indices file')
# parser.add_argument('-nodesStats', action = "store",dest = 'nodesStats', type=str, help='Add nodes stats file')
# parser.add_argument('-catPending', action = "store",dest = 'catPending', type=str, help='Add cat pending file')
# parser.add_argument('-clusterPending', action = "store",dest = 'clusterPending', type=str, help='Add cluster pending file')
# parser.add_argument('-tasks', action = "store",dest = 'tasks', type=str, help='Add tasks file')
# parser.add_argument('-nodes', action = "store",dest = 'nodes', type=str, help='Add nodes file')
# parser.add_argument('-clusterSettings', action = "store",dest = 'clusterSettings', type=str, help='Add cluster settings file')

args = parser.parse_args()

operating_system_dataframe = pandas.DataFrame(columns = ['Diagnostic check','Status','Description'])
cluster_configuration_dataframe = pandas.DataFrame(columns = ['Diagnostic check','Status','Description'])
elasticsearch_usage_stats_dataframe = pandas.DataFrame(columns = ['Diagnostic check','Status','Description'])
pandas.set_option("max_colwidth", 100)
 
urllib3.disable_warnings()

args.CanAccessCluster = 'False'
 
if args.CanAccessCluster=='True':
    if args.secure == 'True':
        if(args.es_url == None):
            print("Please enter Elasticsearch URL!")
            exit()
        else:
            es = elasticsearch.Elasticsearch(args.es_url,connection_class=elasticsearch.RequestsHttpConnection,http_auth = (args.username,args.password),verify_certs=False,timeout=20)
            print(es.info())
            if not es.ping():
                print("Connection failed.\nRecheck the entered URL and credentials!\n")
                exit()
            else:
                print("Connection successful!")       
    elif(args.secure == 'False'):
        if(args.es_url == None):
            print("Enter elasticsearch url!")
            exit()
        else:
            es = elasticsearch.Elasticsearch(args.es_url,connection_class=elasticsearch.RequestsHttpConnection,verify_certs=False,timeout=30)
            if not es.ping():
                print("Connection failed.\nRecheck the entered URL!\n")
                exit()
            else:
                print("Connection successful!")
    
    cat_health = es.cat.health(format = "json",request_timeout=20)
    nodes_stats = es.nodes.stats(format = "json",request_timeout=20)
    cat_nodes = es.cat.nodes( format="json",request_timeout=20)
    cat_shards = es.cat.shards( format="json",request_timeout=20)
    cluster_health = es.cluster.health(format="json",request_timeout=20)
    cat_pending_tasks = es.cat.pending_tasks(format="json",request_timeout=20)
    cluster_pending_tasks = es.cluster.pending_tasks(format="json",request_timeout=20)
    cat_allocation = es.cat.allocation(format="json",request_timeout=20)
    tasks = es.tasks.list(format="json",request_timeout=20)
    cat_indices = es.cat.indices(format="json",request_timeout=20)
    nodes = es.nodes.info(format = "json",request_timeout=20)
    segments = es.indices.segments(format = "json",request_timeout=20)
    cluster_settings = es.cluster.get_settings(include_defaults=True,request_timeout=20)
    
    cluster_name,date_time = GetClusterNameAndDateTime(cat_health)
    cluster_config_df = ClusterConfiguration(nodes,cat_nodes)
    df_overall,disk_data = OverallStats(nodes_stats,cat_allocation)
    
    slow_log = 'No slowlogs'
    
elif args.CanAccessCluster == 'False':

    unzip = ZipFile(args.StatsZipFile)

    file_name = 'tmp/'+args.StatsZipFile.split("_")[3].split(".")[0]+"/"

    cat_health = json.load(unzip.open(file_name+'cat_health.txt'))
    cat_allocation = json.load(unzip.open(file_name+'cat_allocation.txt'))  
    cat_nodes = json.load(unzip.open(file_name+'cat_nodes.txt'))
    cat_shards = json.load(unzip.open(file_name+'cat_shards.txt'))
    cat_indices = json.load(unzip.open(file_name+'cat_indices.txt'))
    cat_pending_tasks = json.load(unzip.open(file_name+'cat_pending_tasks.txt'))
    cluster_pending_tasks = json.load(unzip.open(file_name+'cluster_pending_tasks.txt'))
    tasks = json.load(unzip.open(file_name+'tasks.txt'))
    cluster_health = json.load(unzip.open(file_name+'cluster_health.txt'))
    cluster_settings = json.load(unzip.open(file_name+'cluster_settings.txt'))
    nodes = json.load(unzip.open(file_name+'nodes.txt'))
    nodes_stats = json.load(unzip.open(file_name+'nodes_stats.txt'))  

    with unzip.open(file_name+'es_search_slowlog.json','r') as f:
        response = f.read()
        response = response.replace(b'\n', b'')
        response = response.replace(b'}{', b'},{')
        slow_logs = b"[" + response + b"]"
    slow_logs = json.loads(slow_logs)
    
    cluster_name,date_time = GetClusterNameAndDateTime(cat_health)
    cluster_config_df = ClusterConfiguration(nodes,cat_nodes)
    df_overall,disk_data = OverallStats(nodes_stats,cat_allocation)

    slow_log_dataframe = SlowLog(slow_logs)
    slow_log = slow_log_dataframe.render()
 
       
operating_system_dataframe = CheckCPULoadAverage(operating_system_dataframe,nodes_stats,nodes,'CPU Load Average')
operating_system_dataframe = CheckMemoryUsagePercent(operating_system_dataframe,nodes_stats,'Memory Usage')
operating_system_dataframe = CheckDiskUsageLimits(operating_system_dataframe,cat_allocation,'Disk Usage')
operating_system_dataframe = CheckDiskUsageDistribution(operating_system_dataframe,cat_allocation,'Disk Usage Distribution')
operating_system_dataframe = CheckSwap(operating_system_dataframe,nodes_stats,'Swap')


elasticsearch_usage_stats_dataframe = CheckStatus(elasticsearch_usage_stats_dataframe,cluster_health,'Cluster Health')
elasticsearch_usage_stats_dataframe = CheckIsMasterRequired(elasticsearch_usage_stats_dataframe,cat_nodes,'Is Dedicated Master Required')
elasticsearch_usage_stats_dataframe = CheckRelo(elasticsearch_usage_stats_dataframe,cluster_health,'Indices Relocation Count')
elasticsearch_usage_stats_dataframe = CheckActiveShardsPercentage(elasticsearch_usage_stats_dataframe,cluster_health,'Active Shards Percentage')
elasticsearch_usage_stats_dataframe = CheckShardsDistribution(elasticsearch_usage_stats_dataframe,cat_allocation,'Shards Distribution')
elasticsearch_usage_stats_dataframe,total_shards,total_nodes,total_heap = CheckTotalShardsPerNode(elasticsearch_usage_stats_dataframe,nodes_stats,cat_allocation,'Total Shards Per Node')
elasticsearch_usage_stats_dataframe = CheckShardsPerIndex(elasticsearch_usage_stats_dataframe,cat_indices,'Shards Per Index')
elasticsearch_usage_stats_dataframe = CheckUnassignedShards(elasticsearch_usage_stats_dataframe,cat_shards,'Unassigned Shards Count')
elasticsearch_usage_stats_dataframe = CheckClusterPendingTasks(elasticsearch_usage_stats_dataframe,cluster_pending_tasks,'Cluster Pending Tasks')
elasticsearch_usage_stats_dataframe = CheckTasks(elasticsearch_usage_stats_dataframe,tasks,'High Time-Consuming Tasks')
elasticsearch_usage_stats_dataframe = CheckHeapSizeDataNodes(elasticsearch_usage_stats_dataframe,nodes_stats,'Heap Size for Data Nodes')
elasticsearch_usage_stats_dataframe = CheckHeapSizeMasterNodes(elasticsearch_usage_stats_dataframe,nodes_stats,cat_nodes,'Heap Size for Dedicated Master Nodes')
elasticsearch_usage_stats_dataframe = CheckTotalMemoryDataNodes(elasticsearch_usage_stats_dataframe,nodes_stats,'Total Memory of Data Nodes')  
elasticsearch_usage_stats_dataframe = CheckThreadpoolUsage(elasticsearch_usage_stats_dataframe,nodes_stats,'Threadpool Usage')
elasticsearch_usage_stats_dataframe = CheckOpenFileDescriptors(elasticsearch_usage_stats_dataframe,nodes_stats,'Open File Descriptors')
elasticsearch_usage_stats_dataframe = CheckCircuitBreakers(elasticsearch_usage_stats_dataframe,nodes_stats,'Circuit Breakers')
elasticsearch_usage_stats_dataframe = CheckSegmentFragmentation(elasticsearch_usage_stats_dataframe,cat_indices,'Segment Fragmentation Level')
elasticsearch_usage_stats_dataframe = CheckEmptyIndices(elasticsearch_usage_stats_dataframe,cat_indices,'Empty Indices')


cluster_configuration_dataframe = CheckClusterConcurrentRebalance(cluster_configuration_dataframe,cluster_settings,'Number of Concurrent Shard Rebalance')
cluster_configuration_dataframe = CheckClusterEnableRebalance(cluster_configuration_dataframe,cluster_settings,'Allow Rebalance of all Shards')
cluster_configuration_dataframe = CheckClusterEnableAllocation(cluster_configuration_dataframe,cluster_settings,'Allow Allocation of all Shards')
cluster_configuration_dataframe = CheckAdaptiveReplicaSelection(cluster_configuration_dataframe,cluster_settings,'Use Adaptive Replica Selection')
cluster_configuration_dataframe = CheckUsageOfWildcards(cluster_configuration_dataframe,cluster_settings,'Allow Wildcard Usage')
cluster_configuration_dataframe = CheckAllowLeadingWildcard(cluster_configuration_dataframe,cluster_settings,'Allow Leading Wildcard Usage')
cluster_configuration_dataframe = CheckOpenScrollContext(cluster_configuration_dataframe,cluster_settings,'Open Scroll Context Limit')
cluster_configuration_dataframe = CheckNodeConcurrentRecovery(cluster_configuration_dataframe,cluster_settings,'Node Concurrent Recovery')
cluster_configuration_dataframe = CheckReadOnlyAllowDelete(cluster_configuration_dataframe,cluster_settings,'Any Read Only Indices')
cluster_configuration_dataframe = CheckPainlessRegex(cluster_configuration_dataframe,cluster_settings,'Using Regex in Painless Scripts')

active_shards = cluster_health["active_shards"] 
total_space = total_nodes*25*total_heap

total_checks = len(operating_system_dataframe) + len(cluster_configuration_dataframe) + len(elasticsearch_usage_stats_dataframe)
  
elasticsearch_usage_stats_dataframe['Description'] = elasticsearch_usage_stats_dataframe['Description'].str.replace('\n', '<br>')
operating_system_dataframe['Description'] = operating_system_dataframe['Description'].str.replace('\n', '<br>')
cluster_configuration_dataframe['Description'] = cluster_configuration_dataframe['Description'].str.replace('\n', '<br>')

elasticsearch_usage_stats_dataframe['Description'] = elasticsearch_usage_stats_dataframe['Description'].str.wrap(100)
operating_system_dataframe['Description'] = operating_system_dataframe['Description'].str.wrap(100)
cluster_configuration_dataframe['Description'] = cluster_configuration_dataframe['Description'].str.wrap(100)

styles = [dict(selector = '',props = [("text-align", "center"),('background-color', 'white'),('border-color', 'black'),('border-spacing','2px'),('border','1.5px solid')]),    
            dict(selector= 'th', props= [('font-size', '12px'),('border-style' ,'solid'),('border', '2px solid black'),('border-width', '0.25px'),('height',"25px"),('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'),("font-weight", "normal"), ('vertical-align', 'centre')]),
            dict(selector= "tbody td", props= [("border", "1px solid grey"),('font-size', '12px'),('border-width', '0.25px')])]

elasticsearch_usage_stats_dataframe = elasticsearch_usage_stats_dataframe.style.applymap(color_string,subset = ["Status"]).set_table_styles(styles).set_properties(subset=['Status'], **{'width': '150px'}).set_properties(subset=['Diagnostic check'], **{'width': '250px'}).set_properties(subset=['Description'], **{'text-align': 'left'}).hide_index()
operating_system_dataframe = operating_system_dataframe.style.applymap(color_string,subset = ["Status"]).set_table_styles(styles).set_properties(subset=['Status'], **{'width': '150px'}).set_properties(subset=['Diagnostic check'], **{'width': '250px'}).set_properties(subset=['Description'], **{'text-align': 'left'}).hide_index()
cluster_configuration_dataframe = cluster_configuration_dataframe.style.applymap(color_string,subset = ["Status"]).set_table_styles(styles).set_properties(subset=['Status'], **{'width': '150px'}).set_properties(subset=['Diagnostic check'], **{'width': '250px'}).set_properties(subset=['Description'], **{'text-align': 'left'}).hide_index()

checks = [['Pass',total_pass_checks],['Fail',total_fail_checks],['Warning',total_warning_checks],['UnknownError',total_unknown_error_checks]] 
statement = "Total number of checks : {} <br> Passed: {}; Warning: {}; Failed: {}; UnknownError: {}.<br></br>".format(total_checks,total_pass_checks,total_warning_checks,total_fail_checks,total_unknown_error_checks)
statement += "<p><strong>Cluster Information: <br></strong> Total nodes : {0}<br>Total shards : {1}<br> Active shards : {2}<br>Total space : {3}GB<br>".format(total_nodes,total_shards,active_shards,total_space)
for node in disk_data:
    statement+="Node : {} disk used : {} disk allocated : {}<br>".format(node[0],node[1],node[2])

statement+='</p>'
text = '''

<html>
    <head>
        <style>
            body {{
                    background-color:white;
                }}
            h1  {{
                    color: #0066CC;
                    text-align:center;
                    font-family: Arial;
                    border-style: dotted solid double dashed;
                    border-width: 2px;
                    border-color: green;
                    border-bottom: 3px solid blue;
                    padding-top: 9px;
                    padding-right: 5px;
                    padding-bottom: 9px;
                    padding-left: 5px;
                    font-size: 23px;
                }}
            sub{{
                    font-size: 10px;
                    color: black;
                }}
            h2  {{
                    color: #0066CC;
                    text-align:center;
                    font-family: Arial;
                    font-size: 14px;
                }}
            p   {{
                    font-size:12px;
                    color:black;
                    margin-left:10px;
                }}   
            button {{
                    background-color: #0066CC;
                    border: black;
                    float: left;
                    color: white;
                    width: 50%;
                    padding: 9px 9px;
                    text-align: left;
                    text-decoration: none;
                    display: inline-block;
                    font-size: 14px;
                    margin: 0px 0px;
                    transition-duration: 0.4s;
                    cursor: pointer;
                    box-shadow: 10px 10px 20px grey;
                    border-radius: 12px;
                    font-weight: bold;
                }}
            button:hover {{
                    background-color: #1a8cff;
                }}
            button:active {{
                    background-color: #1a8cff;
                    box-shadow: 0 5px #666;
                    transform: translateY(4px);
                }}
            div1,div2,div3,div4,div5,div6{{
                    width: fit-content;
                    display: inline-block;
                    height: 100px;
                    border: 1px solid blue;
                    
                }}
            Diagnostics{{
                    width: 50%;
                    height: 100%;
                }}
            SlowLogs{{
                    width: 50%;
                    height: 100%;
                }}
            .tablink {{
                    background-color: #1b8dff;
                    text-align: center;
                    color: white;
                    float: left;
                    border: none;
                    outline: none;
                    cursor: pointer;
                    padding: 9.5px 9.5px;
                    font-size: 16px;
                    width: 12.5%;
                    margin-right:10px;
                    border-radius:2px;
                    font-family: Times New Roman;
                    box-shadow: 5px 5px 5px grey;
                    font-weight: bold;
                }}
            .tablink:hover {{
                    background-color: #1a8cff;
                }}
            .tabcontent{{
                    color: white;
                    display: none;
                    padding: 0px 0px;
                    height: 100%;
                    width:100%;  
                }}
            cluster {{
                    text-align:center;
                }}
            #container {{ 
        width: 50%; height: 50%; margin: 0; padding: 0; 
      }} 
        </style>
        <script src="https://cdn.anychart.com/releases/8.10.0/js/anychart-core.min.js"></script>
    <script src="https://cdn.anychart.com/releases/8.10.0/js/anychart-pie.min.js"></script>
    </head>
    <body>
        <h1>Elastic Search Diagnostics Report<br> <sub>cluster name : {0}</sub><br><sub>date & time : {1}</sub></h1>
        <button class="tablink" onclick="openPage('Diagnostics')" id="defaultOpen">Diagnostics</button>
        <button class="tablink" onclick="openPage('SlowLogs')" >Slow Logs</button>  
        <div id="SlowLogs" class="tabcontent">
            <br>
            <h2><u>Slow Logs</u></h2><br></br>{8}<br></br>
        </div>
        <div id="Diagnostics" class="tabcontent">
            <br></br>
            <h2><u> Elastic Search Diagnostics </u></h2><br></br>
            <article>
                <button class="button" onclick="hide1()">Summary</button> 
                <br></br>
                <script>
                    function hide1() {{
                      var x = document.getElementById("div1");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script>
                <div class="hide1" id="div1" style="display:block;" >  <br></br>   
                    <div id="container"></div>
                    <script>
                    var palette = anychart.palettes.distinctColors();
                    palette.items([
                      {{ color: '#1dd05d' }},
                      {{ color: '#f60000' }},
                      {{ color: '#ffa000' }},
                      {{ color: '#156ef2' }}
                    ]);                   
                    var data = anychart.data.set({9});
                    var chart = anychart.pie(data)
                    chart.innerRadius('55%');
                    chart.title('Checks')
                    chart.container('container');
                    chart.palette(palette);
                    chart.draw();
                    </script>
                    <p><strong>Cluster Checks:<br><br></strong> {2} </br></p>
                </div>
                
            </article>
            &nbsp;&nbsp;&nbsp;
            <article>       
                <article>
                    <button onclick="hide2()">Cluster configuration</button> 
                    <br></br>
                    <script>
                        function hide2() {{
                          var x = document.getElementById("div2");
                          if (x.style.display === "none") {{
                            x.style.display = "block";
                          }} else {{
                            x.style.display = "none";
                          }}
                        }}
                    </script>
                    <div class="hide2" id="div2" style="display:none;" ><br><br> {3} </br></div>
                </article>
                &nbsp;&nbsp;
                <article>
                    <button onclick="hide3()">Elastic search overall stats usage</button> 
                    <br></br>
                    <script>
                        function hide3() {{
                          var x = document.getElementById("div3");
                          if (x.style.display === "none") {{
                            x.style.display = "block";
                          }} else {{
                            x.style.display = "none";
                          }}
                        }}
                    </script>
                    <div class="hide3" id="div3" style="display:none;" > <br> </br> {4} </div>
                </article>    
            </article>
            <br> <hr> <br>
        <article>    
            &nbsp;
            <article>
                <button onclick="hide4()">Operating system checks</button> 
                <br>
                <script>
                    function hide4() {{
                      var x = document.getElementById("div4");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script>
                <div class="hide4" id="div4" style="display:none;" > <br> <br> {5} </br></div>
            </article>
            &nbsp;&nbsp;&nbsp;
            <br></br>
            <article> 
                <button onclick="hide5()">Cluster configuration checks</button>   
                <br>
                <script>
                    function hide5() {{
                      var x = document.getElementById("div5");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script> 
                <div class="hide5" id="div5" style="display:none;"> <br> <br> {6} </br></div>
            </article>
            &nbsp;&nbsp;&nbsp; 
            <br></br>
            <article> 
                <button onclick="hide6()">Elastic search stats usage checks</button>
                <br>
                <script>
                    function hide6() {{
                      var x = document.getElementById("div6");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script>  
                <div class="hide6" id="div6" style="display:none;"> <br> <br> {7} </br> </div>
            </article>
            <br style = “line-height:50; > </br>
            <hr>
            <br style = “line-height:1000; > </br>
        </article>
        </div>
        
        <script>
            function openPage(pageName) {{
              var i, tabcontent, tablinks;
              tabcontent = document.getElementsByClassName("tabcontent");
              for (i = 0; i < tabcontent.length; i++) {{
                tabcontent[i].style.display = "none";
              }}
              document.getElementById(pageName).style.display = "block";
            }}
            document.getElementById("defaultOpen").click();
        </script>   
    </body>
</html>
'''.format(cluster_name,str(date_time),statement,cluster_config_df.render(),df_overall.render(),operating_system_dataframe.render(),cluster_configuration_dataframe.render(),elasticsearch_usage_stats_dataframe.render(),slow_log,checks)

main_file_name = 'PSRElasticSearchDiagnosticsReport.html'
path = os.getcwd()
file_path = os.path.join(path,folder_name)
with open(os.path.join(file_path, main_file_name), 'w',encoding='utf-8') as fp:
    fp.write(text)
    fp.close()
    
shutil.make_archive(zip_folder_name, 'zip', folder_name)
