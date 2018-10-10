
def generateLog(batchOfTransactions, hasCycle, idsInBlock):
    # primeiramente crio um dicionario com os atributos como chave
    dictPossibleAttr = getBatchVariables([batchOfTransactions])

    if hasCycle:
        splitBatchs(batchOfTransactions, idsInBlock, dictPossibleAttr)
    else:
        # print(list(batchOfTransactions))
        # splitBatchs([batchOfTransactions], idsInBlock, dictPossibleAttr)
        pass
        

def splitBatchs(batchOfTransactions, idsInBlock, dictPossibleAttr):
    transactionsList = list()
    currentTransactionList = list()

    for id in idsInBlock:
        for transaction in batchOfTransactions:
            if transaction['id'] == id:
                currentTransactionList.append(transaction)
        transactionsList.append(currentTransactionList)
        currentTransactionList = []
    
    logText(transactionsList, dictPossibleAttr)

def logText(listOfTransactions, dictPossibleAttr):

    dictValues = {'searchIndex' : 0,
                'newTs' : 0,
                'dictOfVariables' : dictPossibleAttr}
    
    for eachBlock in listOfTransactions:
        for transaction in eachBlock:
            dictValues['newTs'] = makeLog(eachBlock, transaction, dictValues)

def makeLog(eachBlock, transaction, dictValues):
    newTs = 0
    update = False
    if dictValues['newTs'] != 0:
        newTs = dictValues['newTs']
        update = True
    else:
        newTs = transaction['timestamp']
    # assumo que sempre vai começar com uma leitura
    if ((transaction['op'] == 'R') or (transaction['op'] == 'r')):
    # checar se existe alguma escrita neste atributo durante o processo
        for i in range(1, len(eachBlock)):
            if (((eachBlock[i]['op'] == 'W') or (eachBlock[i]['op'] == 'w')) and 
                (eachBlock[i]['at'] == transaction['at'])):
                    print('{0};T{1};start'.format(newTs, transaction['id']))

    elif ((transaction['op'] == 'W') or (transaction['op'] == 'w')):
        if dictValues['dictOfVariables'][transaction['at']] == 0:
            # atualizo o valor no dicionario
            dictValues['dictOfVariables'][transaction['at']] = transaction['newValue']
            print('{0};T{1};{2};NULL;{3}'.format(newTs, transaction['id'], 
                transaction['at'].upper(), transaction['newValue']))
        else:
            print('{0};T{1};{2};{3};{4}'.format(newTs, transaction['id'], 
                transaction['at'].upper(), dictValues['dictOfVariables'][transaction['at']], transaction['newValue']))
            dictValues['dictOfVariables'][transaction['at']] = transaction['newValue'] + dictValues['dictOfVariables'][transaction['at']]
    
    elif (((transaction['op'] == 'C') or (transaction['op'] == 'c')) or 
    ((transaction['op'] == 'A') or (transaction['op'] == 'a'))):
        if((transaction['op'] == 'C') or (transaction['op'] == 'c')):
            print('{0};T{1};commit'.format(newTs, transaction['id']))
        else:
            print('{0};T{1};abort'.format(newTs, transaction['id']))
        dictValues['newTs'] = newTs + 1
    
    if update:
        dictValues['newTs'] = dictValues['newTs'] + 1
    return dictValues['newTs'] 
        
def getBatchVariables(listOfTransactions):
    dictOfVariables = dict()
    listOfVariables = list()
    
    for eachBlock in listOfTransactions:
        for transaction in eachBlock:

            print(transaction['at'])
            if (transaction['at'] not in listOfVariables) and (transaction['at'] != '-'):
                listOfVariables.append(transaction['at'])
    # populo o dict
    for att in listOfVariables:
        dictOfVariables[att] = 0
    print(dictOfVariables)
    return dictOfVariables
    