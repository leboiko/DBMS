import os

class MakeJournal:

    def __init__(self, batchOfTransactions, hasCycle, idsInBlock, dictPossibleAttr, startIndex=0):
        self.batchOfTransactions = batchOfTransactions
        self.hasCycle = hasCycle
        self.idsInBlock = idsInBlock
        self.dictPossibleAttr = dictPossibleAttr
        self.startIndex = startIndex

    def generateLog(self):
        self.splitBatchs(self.batchOfTransactions)
        # print(self.dictPossibleAttr)
            
    def splitBatchs(self, batchOfTransactions):
        transactionsList = list()
        currentTransactionList = list()
        for id in self.idsInBlock:
            for transaction in batchOfTransactions:
                if transaction['id'] == id:
                    currentTransactionList.append(transaction)
            transactionsList.append(currentTransactionList)
            currentTransactionList = [] 
        self.logText(transactionsList)

    def logText(self, listOfTransactions):
        for eachBlock in listOfTransactions:
            for transaction in eachBlock:
                self.makeLog(eachBlock, transaction)

    # Esta função checa se o timestamp da transacao atual é maior do que o registro salvo
    # caso seja, o timestamp corrente é gravado na variavel startIndex
    def checkTs(self, transaction):
        if transaction['timestamp'] > self.startIndex:
            self.startIndex = transaction['timestamp']

    def writeToFile(self, string):
        try:
            with open("archive.log", "a") as outfile:
                outfile.write(string)
        except IOError:
            print('Error while writeng the log file!')

    def searchIdInFile(self, fileName, id, op='start'):
        recordExists = False
        if os.path.isfile(fileName):
            try:
                with open(fileName, "r") as outfile:
                    for line in outfile:
                        listValues = line.strip().split(';')
                        if ((listValues[1] == 'T'+str(id)) and (listValues[2] == op)):
                            recordExists = True
            except IOError:
                print('Error while writeng the log file!')
        return recordExists
    
    def makeLog(self, eachBlock, transaction):

        stringLine = ''
        self.checkTs(transaction)
        # assumo que sempre vai começar com uma leitura
        if transaction['op'].upper() == 'R':
            for i in range(1, len(eachBlock)):
                if ((eachBlock[i]['op'].upper() == 'W') and 
                    (eachBlock[i]['at'] == transaction['at'])):
                        if not self.searchIdInFile('archive.log', transaction['id']):
                            stringLine = '{0};T{1};start\n'.format(self.startIndex, transaction['id'])

        elif transaction['op'].upper() == 'W':
            if self.dictPossibleAttr[transaction['at']] == 0:
                value = 'NULL'
            else:
                value = self.dictPossibleAttr[transaction['at']]

            stringLine = '{0};T{1};{2};{3};{4}\n'.format(self.startIndex, transaction['id'], 
                transaction['at'].upper(), value, transaction['newValue'])
            
            if not self.searchAbort(transaction['id'], transaction['at'], eachBlock):
                if transaction['newValue'] != '-':
                    self.dictPossibleAttr[transaction['at']] = int(transaction['newValue'])
        
        elif ((transaction['op'].upper() == 'C') or (transaction['op'].upper() == 'A')):
            if transaction['op'].upper() == 'C':
                if self.searchWrite(transaction['id'], transaction['at'], eachBlock):
                    stringLine = '{0};T{1};commit\n'.format(self.startIndex, transaction['id'])
                    
            else:
                stringLine = '{0};T{1};abort\n'.format(self.startIndex, transaction['id'])
        # Se tem uma linha para imprimir para o arquivo, imprime        
        if stringLine != '' and not self.currentLineInLog('archive.log', stringLine):   
            self.writeToFile(stringLine)
        self.startIndex = self.startIndex + 1
    
    
    def currentLineInLog(self, fileName, line):
        recordExists = False
        if os.path.isfile(fileName):
            toCompare = line.split(';')[1:]
            # removo o \n
            # print(toCompare)
            toCompare[-1] = toCompare[-1][:-1]
            # print(toCompare)
            try:
                with open(fileName, "r") as outfile:
                    for line in outfile:
                        listValues = line.strip().split(';')[1:]
                        # print(listValues, toCompare)
                        # print(listValues, toCompare)
                        if listValues == toCompare:
                            print('true motherfocker!')
                            
                            recordExists = True
            except IOError:
                print('Error while writeng the log file!')
        return recordExists

    def currentIndex(self):
        return self.startIndex
    # Se where for 0 procura apenas no bloco, caso for 1 procura no archive.log
    def searchWrite(self, id, attr, eachBlock):
        hasWrite = False
        for i in range(0, len(eachBlock)):
            if ((eachBlock[i]['op'].upper() == 'W') and (eachBlock[i]['id'] == id)):
                # if self.searchIdInFile('archive.log', eachBlock[i]['id'], op='commit'):
                hasWrite = True
        return hasWrite

    def searchAbort(self, id, attr, eachBlock):
        hasAbort = False
        for i in range(0, len(eachBlock)):
            if ((eachBlock[i]['op'].upper() == 'A') and (eachBlock[i]['id'] == id)):
                hasAbort = True
        return hasAbort
            
    def getBatchVariables(self, listOfTransactions):
        dictOfVariables = dict()
        listOfVariables = list()
        
        for eachBlock in listOfTransactions:
            for transaction in eachBlock:
                if (transaction['at'] not in listOfVariables) and (transaction['at'] != '-'):
                    listOfVariables.append(transaction['at'])
        # populo o dict
        for att in listOfVariables:
            dictOfVariables[att] = 0
        # print(dictOfVariables)
        return dictOfVariables
        