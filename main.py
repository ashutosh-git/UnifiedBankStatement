import csv
from csv import DictReader
from datetime import datetime

#defined unified structure for bank stagement
parsed_schema_list=['bank_code','trx_date','trx_type','amounts','to','from']

def date_converter(bank_code,timestamp):
    '''

    :param bank_code: defined bank code per bank
    :param timestamp: input date field per bank
    :return: generic datetime
    '''
    date_pharse={'bank1':lambda timestamp :datetime.strptime(timestamp, '%b %d %Y'),#Oct 1 2019
                 'bank2':lambda timestamp :datetime.strptime(timestamp, '%d-%m-%Y'),#03-10-2019
                 'bank3':lambda timestamp :datetime.strptime(timestamp, '%d %b %Y')}#5 Oct 2019

    return date_pharse[bank_code](timestamp).strftime('%d %b %Y')

def euro_cent_add(euro,cents):
    return(int(euro)+int(cents)/100)

def validate_csv_schema(trx_csv):
    '''
    :param trx_csv: Bank statement csv file
    :return: {bank_code,schema} in dict
    '''
    #defined schema
    bank1=['timestamp','type','amount','from','to']
    bank2=['date','transaction','amounts','to','from']
    bank3=['date_readable','type','euro','cents','to','from']
    with open(trx_csv,'r',newline='\n') as trxcsv:
        trx_rows = csv.reader(trxcsv, delimiter=',')
        header = next(trx_rows)
        trxcsv.close()

    if bank1==header:
        return {'bank_code':'bank1','schema':bank1}
    elif bank2==header:
        return {'bank_code':'bank2','schema':bank2}
    elif bank3==header:
        return {'bank_code':'bank3','schema':bank3}
    else:
        raise Exception("Schema Not matched Validation")

def bank_trx_data_parse(bank_code,row_dict):
    '''
    :param bank_code: defined bank code per bank
    :param row_dict: input row of that bank code
    :return: parsed trx details in dict
    '''
    parsed_mapping_dict=None
    if bank_code=='bank1':
        parsed_mapping_dict={'bank_code':bank_code,
                             'trx_date':date_converter(bank_code,row_dict['timestamp']),
                             'trx_type':row_dict['type'],
                             'amounts':row_dict['amount'],
                             'to':row_dict['to'],
                             'from':row_dict['from']}
    elif bank_code=='bank2':
        parsed_mapping_dict={'bank_code':bank_code,
                             'trx_date':date_converter(bank_code,row_dict['date']),
                             'trx_type':row_dict['transaction'],
                             'amounts':row_dict['amounts'],
                             'to':row_dict['to'],
                             'from':row_dict['from']}

    elif bank_code=='bank3':
        parsed_mapping_dict={'bank_code':bank_code,
                             'trx_date':date_converter(bank_code,row_dict['date_readable']),
                             'trx_type':row_dict['type'],
                             'amounts':euro_cent_add(row_dict['euro'],row_dict['cents']),
                             'to':row_dict['to'],
                             'from':row_dict['from']}


    #validation to check every bank mapping follow unified schema
    if not set(parsed_schema_list)== set([column for column in parsed_mapping_dict.keys()]):
        raise Exception("Error in data parse for Bank Code",bank_code)

    return parsed_mapping_dict

def read_bank_transaction(list_trx_csv_file):
    '''
    :param list_trx_csv_file: list of file location
    :return: file read iterator
    '''
    #validate all csv file have valid header
    #this function on validate 1st row as header of all the provided file
    #need to validate all csv file before parse and merge
    for csv_file in list_trx_csv_file:
        validate_csv_schema(csv_file)

    for csv_file in list_trx_csv_file:
        #get the bank code
        trx_csv_details=validate_csv_schema(csv_file)
        trxcsv=open(csv_file,'r',newline='\n')
        csv_dict_reader = DictReader(trxcsv)
        for row in csv_dict_reader:
            #parse the file to defined unified structure
            yield bank_trx_data_parse(trx_csv_details['bank_code'],row)
            #testing
            #print(bank_trx_data_parse(trx_csv_details['bank_code'],row))

def save_unified_trx_csv(bank_trx_csv_list):
    #This function can be further modified to save in Database

    #read all the input bank transaction csv file
    parser_itr=read_bank_transaction(bank_trx_csv_list)

    #Save the unified csv file into target file
    target_file=open('./UnifiedTrxOutput/all_bank_trx.csv','w',newline='')
    header = parsed_schema_list
    writer = csv.DictWriter(target_file, fieldnames=header)
    writer.writeheader()
    for row in parser_itr:
        writer.writerow(row)
        #print(row)

def execute_unified_bank_statement():
    #list of bank csv file to be unified
    bank_trx_csv_list=['./BankData/bank1.csv','./BankData/bank2.csv','./BankData/bank3.csv']

    #Save unified file function call
    save_unified_trx_csv(bank_trx_csv_list)

#function call
execute_unified_bank_statement()