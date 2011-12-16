Cloud files command line interface

This is a tool to control your cloud files storage via command line interface. 

Motivation

1. There is no command line tool capable to upload directory tree to your container. 
   I need it to deploy my application static and media files.
2. There is no tool with ability to remove/upload files in multithreaded mode. 
   Single thread is a bottleneck of process when you try to upload thousands of files.
   For 20 000 - 30 000 small files it could take weeks with no garanty of success.
3. Some times CF API crashes with some errors but task could be finished with another 
   try. I need tool to do that automatically. 
   
TODO:
1. Directory tree syncronization upload
2. Arguments simplification and clearance
3. Setup tools installation configuration

Examples:

./cfcli.py create cfcli -u=lazarev -k=<key>
./cfcli.py upload cfcli . -l=1 -u=lazarev -k=<key> -f=cfcli_upload1.log
./cfcli.py delete cfcli -l 1 -u=lazarev -k=<key> -f=cfcli_d1.log