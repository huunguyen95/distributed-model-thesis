# distributed-model-thesis
This is repository for my master's thesis about dynamic distributed-system using KAFKA and KSQL


#Model architecture
![image](https://user-images.githubusercontent.com/22879863/167240912-98ee3fa9-5e60-49ab-8d0d-ee53a5035379.png)


#Code structure
- At Center server: /src/center-server/
- At Analyzer: /src/analyzer/
- At IOT agent: /src/agent/
- At dashboard: /src/dashboard
- Input: /input
- output: /output

#SYSTEM INFO:
- Center-server:
Cpu 16
Ram 32
Storage 100
- Analyzer:
Cpu2
Ram 6
Storage 30
- Agent:
Cpu 1
Ram 2
Storage 30

#NETWORK
- PING OPEN
- INTERNAL NETWORK CONNECTION

![image](https://user-images.githubusercontent.com/22879863/171525035-2201efcb-38fd-4ca3-ae27-4279018490d4.png)


#RUN Code
1. Change permission for all folder
chown -R [user][user_group] folder
2. Change permission for ssh key file
chmod 400 [ssh_key]
3. Modify environment
vim .env
4. Run at ground InformationStrategy
nohup python3 InformationStrategy.py &
5. Run socket_server
6. Run TransferStategy
faust -A data_process_app worker -l info

