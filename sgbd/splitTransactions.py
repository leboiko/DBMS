from sgbd import serialityTest as sTest

class SplitTransactions:
    """
        Esta classe é responsável por separar os blocos de transações e chamar os testes de seriabilidade
        para determinar quais batchs de transações concorrentes podem ser executadas em modo serializado
    """
    def __init__(self, listTransactions, listIds, dictVariables={}):
        """
            Construtor de um novo objeto 'SplitTransactions'

            :listTransactions: Lista com as transações no formato de dicionário 
                                    { 'timestamp' : int,
                                    'id' : int,
                                    'op' : str,
                                    'at' : str,
                                    'newValue' : int}.
            :listIds: Lista com os identificadores das transações no bloco.
            :dictVariables: dicionário com os valores de atributos atuais
        """
        self.listTransactions = listTransactions
        self.listIds = listIds
        self.dictVariables = dictVariables
    
    def split(self):
        """
            Esta função é chamada externamente para gerar os registros de log. 
            Ao final, o currentIndex é atualizado.
        """

        batchsTransactionsIds = self.cleanTheMess(self.generateListOfBatchs(
            self.checkBirthAndDeath(
                self.listTransactions, self.listIds)))

        self.dictVariables = self.getBatchVariables([self.listTransactions])
        newBatch = list()
        startIndex = 0

        for batch in batchsTransactionsIds:
            for transaction in self.listTransactions:
                if transaction['id'] in batch:
                    newBatch.append(transaction)
            serialityChecking = sTest.SerialityTest(newBatch, batch, self.dictVariables, startIndex)
            serialityChecking.checkConditions()
            startIndex = serialityChecking.currentIndex - 1
            newBatch = []
   
    def getDictVariables(self):
        """
            Esta função é utilizada para acessar o dicionário com os valores atuais dos atributos
            externamente, retorna o dicionário no formato {'ATRIBUTO' : int}
        """
        return self.dictVariables

    def cleanTheMess(self, listOfIdsInBatchs):
        """
            Esta função recebe:

            :listOfIdsInBatch: lista contendo listas com ID'S de possíveis blocos concorrentes

            E reorganiza os os ID's pertencentes a cada bloco. Ao final retorna uma lista
            com os ID's pertencentes a cada bloco de transações, como no formato exemplo:
            [[1, 2][3, 4, 5]]
        """
        indexToRemove = []
        for i in range(0, len(listOfIdsInBatchs)):
            partOne = set(listOfIdsInBatchs[i])
            for j in range(i+1, len(listOfIdsInBatchs)):
                partTwo = set(listOfIdsInBatchs[j])
                if((len(list(partOne & partTwo)) > 0) and 
                (j not in indexToRemove)):
                    indexToRemove.append(j)
        for index in sorted(indexToRemove, reverse=True):
            del listOfIdsInBatchs[index]

        return listOfIdsInBatchs

    def getBatchVariables(self, listOfTransactions):
        """
            Esta função recebe:

            :listOfTransactions: lista de transações contidas no bloco

            E retorna um dicionário contendo os atributos como chave e seus respectivos
            valores associados
        """
        dictOfVariables = dict()
        listOfVariables = list()
            
        for eachBlock in listOfTransactions:
            for transaction in eachBlock:
                if (transaction['at'] not in listOfVariables) and (transaction['at'] != '-'):
                    listOfVariables.append(transaction['at'])

        for att in listOfVariables:
            dictOfVariables[att] = 0

        return dictOfVariables

    
    def generateListOfBatchs(self, dictIdsLifetimes):
        """
            Esta função recebe:

            :dictIdsLifetimes: dicionário contendo o ID como chave e uma tupla (TimestampNascimento, TimestampMorte) associada

            E retorna uma lista de listas detransações por bloco, como no exemplo: [[1, 2], [3, 4, 5]].
        """
        listIds = sorted(list(dictIdsLifetimes))
        currentDeath = 0
        graphDict = dict()

        for i in range(0, len(listIds)):
            edgesList = list()
            if currentDeath == 0: 
                currentDeath = dictIdsLifetimes[listIds[i]][1]
            elif dictIdsLifetimes[listIds[i]][1] > currentDeath:
                currentDeath = dictIdsLifetimes[listIds[i]][1]
            for j in range(i, len(listIds)):
                if (dictIdsLifetimes[listIds[j]][0] < currentDeath):
                    edgesList.append(listIds[j])
            
            if len(edgesList) == 1:
                if listIds[i] != edgesList[0]:
                    graphDict[listIds[i]] = edgesList
            else:
                graphDict[listIds[i]] = edgesList

            edgesList = []

        return list(graphDict.values())


    def checkBirthAndDeath(self, listOfTransactions, listOfIds):
        """
            Esta função recebe:

            :listOfTransactions: lista de transações no bloco
            :listOfIds: lista de ID's das transações no bloco

            E retorna um dicionário contendo o ID como chave e uma tupla (TimestampNascimento, TimestampMorte) associada
        """
        
        dictBirthAndDeath = dict()
        birth = 0
        death = 0
        for id in listOfIds:
            for transaction in listOfTransactions:
                if transaction['id'] == id:
                    if id not in dictBirthAndDeath:
                        if transaction['op'].upper() != 'C':
                            birth = transaction['timestamp']
                    elif transaction['op'].upper() == 'C':
                            death = transaction['timestamp']
                    dictBirthAndDeath[id] = (birth, death)

        return dictBirthAndDeath