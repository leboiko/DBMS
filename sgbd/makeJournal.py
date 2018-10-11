import os

class MakeJournal:
    """
        Esta classe é responsável por gerar o arquivo de logs do banco
    """
    def __init__(self, batchOfTransactions, hasCycle, idsInBlock, dictPossibleAttr, startIndex=0, fileName='archive.log'):
        """
            Construtor de um novo objeto 'MakeJournal'

            :batchOfTransactions: Batch de transações para gerar log.
            :hasCycle: Booleano informando se existe ciclo no grafo destas transações.
            :idsInBlock: Identificadores das transações no bloco.
            :dictPossibleAttr: Um dicionário com os atributos ativos no bloco e seu respectivo valor.
            :startIndex: Indice inicial para os registros de log. Caso não seja inicializado o valor default 0 é utilizado.
            :fileName: Arquivo para realizar a escrita dos logs. Caso não seja inicializado o valor default archive.log é utilizado.
        """
        self.batchOfTransactions = batchOfTransactions
        self.hasCycle = hasCycle
        self.idsInBlock = idsInBlock
        self.dictPossibleAttr = dictPossibleAttr
        self.startIndex = startIndex
        self.fileName = fileName

    def generateLog(self):
        """
            Esta é a função que é chamada externamente pela classe 'SerialityTest'
            para geração dos logs. Ela verifica o atributo :hascycle e caso
            o mesmo seja True, a função 'splitBatchs' é chamada, caso contrário,
            a função que gera os logs 'logText' é chamada
        """
        if self.hasCycle:
            self.splitBatchs()
        else:
            self.logText([self.batchOfTransactions])
    
            
    def splitBatchs(self):
        """
            Esta é a função divide o batch de transações atual em listas
            de transações com o mesmo Id e ao final chama a função 'logText''
            que faz a geração dos registros de log
        """
        transactionsList = list()
        currentTransactionList = list()
        for id in self.idsInBlock:
            for transaction in self.batchOfTransactions:
                if transaction['id'] == id:
                    currentTransactionList.append(transaction)
            transactionsList.append(currentTransactionList)
            currentTransactionList = [] 
        self.logText(transactionsList)

    def logText(self, listOfTransactions):
        """
            Esta é a função recebe uma lista contendo listas de transações
            a serem fixadas nos registrosde log.

            :listOfTransactions: segue o formato [[T1], [T2], ... , [Tn]]
            Sendo Tn = [{ts1}, {ts2}, ..., {tsn}], onde :tsn: é a transação corrente no bloco

            Para cada bloco T da lista de transações a função makeLog é chamada.
        """
        for eachBlock in listOfTransactions:
            for transaction in eachBlock:
                self.makeLog(eachBlock, transaction)

    
    def checkTs(self, transaction):
        """
            Esta função checa se o timestamp da transacao atual é maior do que o registro salvo
            caso seja, o timestamp corrente é gravado na variavel startIndex
        """
        if transaction['timestamp'] > self.startIndex:
            self.startIndex = transaction['timestamp']
            if transaction['timestamp'] <= int(self.getLastTs()): 
                self.startIndex = int(self.getLastTs()) + 1

    def writeToFile(self, line):
        """
            Esta função recebe 
            
            :line: string formatada em um dos seguintes formatos:

            TS;Ti;start
            TS;Ti;commit
            TS;Ti;abort
            TS;Ti;Xj;V1;V2

            e realiza a escrita no arquivo de log.
        """
        try:
            with open(self.fileName, "a") as outfile:
                outfile.write(line)
        except IOError:
            print('Erro ao ler o arquivo de logs!')

    def searchIdInFile(self, id, op='start'):
        """
            Esta função recebe um id e o tipo de operação (opcional) e busca no arquivo de log se já existe
            um registro para esta transação, retornando False caso não exista e True caso exista.
        """
        recordExists = False
        if os.path.isfile(self.fileName):
            try:
                with open(self.fileName, "r") as outfile:
                    for line in outfile:
                        listValues = line.strip().split(';')
                        if ((listValues[1] == 'T'+str(id)) and (listValues[2] == op)):
                            recordExists = True
            except IOError:
                print('Erro ao ler o arquivo de logs!')

        return recordExists

    def getLastTs(self):
        """
            Esta função retorna o último timestamp gravado no arquivo de log
        """
        lastTs = 0
        if os.path.isfile(self.fileName):
            try:
                with open(self.fileName, "r") as outfile:
                    for line in outfile:
                        lastTs = line.strip().split(';')[0]
            except IOError:
                print('Erro ao ler o arquivo de logs!')

        return lastTs
    
    def makeLog(self, eachBlock, transaction):
        """
            Esta função efetivamente cria os registros para o log. Ela recebe dois valores.

            :eachblock: o bloco T atual de transações
            :transaction: a transação atual {ts} para ser gravada no log

            ao término da execução da função, o startIndex é atualizado.

        """
        stringLine = ''
        self.checkTs(transaction)
        
        if transaction['op'].upper() == 'R':
            for i in range(1, len(eachBlock)):
                if ((eachBlock[i]['op'].upper() == 'W') and 
                    (eachBlock[i]['at'] == transaction['at'])):
                        if not self.searchIdInFile(transaction['id']):
                            stringLine = '{0};T{1};start\n'.format(self.startIndex, transaction['id'])

        elif transaction['op'].upper() == 'W':
            if not self.searchIdInFile(transaction['id']):
                self.writeToFile('{0};T{1};start\n'.format(self.startIndex, transaction['id']))
                self.startIndex += 1

            if self.dictPossibleAttr[transaction['at']] == 0:
                value = 'NULL'
            else:
                value = self.dictPossibleAttr[transaction['at']]

            if transaction['newValue'] == '-':
                stringLine = '{0};T{1};{2};{3};{4}\n'.format(self.startIndex, transaction['id'], 
                    transaction['at'].upper(), value, 'NULL')
            else:
                stringLine = '{0};T{1};{2};{3};{4}\n'.format(self.startIndex, transaction['id'], 
                    transaction['at'].upper(), value, transaction['newValue'])
            
            if not self.searchOp(transaction['id'], transaction['at'], eachBlock, 'A'):
                if transaction['newValue'] != '-':
                    self.dictPossibleAttr[transaction['at']] = int(transaction['newValue'])
        
        elif ((transaction['op'].upper() == 'C') or (transaction['op'].upper() == 'A')):
            if transaction['op'].upper() == 'C':
                if self.searchOp(transaction['id'], transaction['at'], eachBlock, 'W'):
                    stringLine = '{0};T{1};commit\n'.format(self.startIndex, transaction['id'])     
            else:
                stringLine = '{0};T{1};abort\n'.format(self.startIndex, transaction['id'])

        if stringLine != '' and not self.currentLineInLog(stringLine):   
            self.writeToFile(stringLine)

        self.startIndex = self.startIndex + 1
        
    
    
    def currentLineInLog(self, line):
        """
            Esta função recebe 
            
            :line: string formatada em um dos seguintes formatos:

            TS;Ti;start
            TS;Ti;commit
            TS;Ti;abort
            TS;Ti;Xj;V1;V2

            e retorna True, caso exista a uma linha exatamente igual, desconsiderando o timestamp,
            nos registros de log, ou False caso contrário.

        """
        recordExists = False
        if os.path.isfile(self.fileName):
            toCompare = line.split(';')[1:]
            toCompare[-1] = toCompare[-1][:-1]
            try:
                with open(self.fileName, "r") as outfile:
                    for line in outfile:
                        listValues = line.strip().split(';')[1:]
                        if listValues == toCompare:
                            recordExists = True
            except IOError:
                print('Erro ao ler o arquivo de logs!')
        return recordExists

    def currentIndex(self):
        """
            Esta função retorna o startIndex corrente. Foi criada exclusivamente para ser acessada externamente.
        """
        return self.startIndex
    

    def searchOp(self, id, attr, eachBlock, op):
        """
            Esta função recebe 
            
            :id: identificador da transação
            :attr: o atributo a ser analisado
            :eachBlock: o bloco de transações para busca
            :op: string que representa a operação a ser analisada ('C', 'A', 'R', 'W')

            e retorna True, caso exista a a operação op para o atributo attr identificada pelo id, ou 
            False caso contrário.
        """
        hasWrite = False
        for i in range(0, len(eachBlock)):
            if ((eachBlock[i]['op'].upper() == op) and (eachBlock[i]['id'] == id)):
                hasWrite = True
        return hasWrite

        