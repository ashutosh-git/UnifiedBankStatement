# UnifiedBankStatement

### Requirement
python 3.8
package: csv, datetime

### Assumptions
For all bank the transactions are in currency "Euro" only.


### Idea
Let's create a unified bank statement schema to merge all csv
['bank_code','trx_date','trx_type','amounts','to','from']

bank_code: Is to identify from which bank this transaction is
trx_date: Parsed all date format to a common format "02 Oct 2019"
trx_type: Either have add/remove value
amounts:  Parsed euro, cents to amounts, keeps others as same
to: keep same as incoming file
from: keep same as incoming file  


### Run script
All the functions are there in main function
you can run by following command

cd C:/Users/ashutosh/IdeaProjects/UnifiedBankStatement
C:\Users\ashutosh\AppData\Local\Programs\Python\Python38\python.exe C:/Users/ashutosh/IdeaProjects/UnifiedBankStatement/main.py

target file will be saved in C:/Users/ashutosh/IdeaProjects/UnifiedBankStatement/UnifiedTrxOutput



### output of unified bank statement
bank_code,trx_date,trx_type,amounts,to,from
bank1,01 Oct 2019,remove,99.20,182,198
bank1,02 Oct 2019,add,2000.10,198,188
bank2,03 Oct 2019,remove,99.40,182,198
bank2,04 Oct 2019,add,2123.50,198,188
bank3,05 Oct 2019,remove,5.07,182,198
bank3,06 Oct 2019,add,1060.08,198,188