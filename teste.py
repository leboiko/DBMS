import fileinput
from serialityTest import SerialityTest
import os


def getBatchVariables(listOfTransactions):
        dictOfVariables = dict()
        listOfVariables = list()
        
        for eachBlock in listOfTransactions:
            for transaction in eachBlock:
                if (transaction['at'] not in listOfVariables) and (transaction['at'] != '-'):
                    listOfVariables.append(transaction['at'])
        # populo o dict
        for att in listOfVariables:
            dictOfVariables[att] = 0
        print(dictOfVariables)
        return dictOfVariables

# Esta função recebe o dicionario gerado pela funcao checkBirthAndDeath e
# retorna uma lista de batchs transações concorrentes
def generateListOfBatchs(dictIdsLifetimes):
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

# Esta função retorna um dicionário sendo que as chaves são os ID'S das 
# transações e o valor é uma tupla correspondente ao (IndiceNascimento, IndiceMorte)
def checkBirthAndDeath(listOfTransactions, listOfIds):
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

def deletePreviousLog(fileName):
    if os.path.isfile(fileName):
        os.remove(fileName)

if __name__ == '__main__':
    deletePreviousLog('archive.log')
    # print(currentPath)
    # Criar uma lista que irá guardar os hashmaps
    listTransactions = list()
    # Crio uma lista que irá guardas os id's unicos de transações
    listIds = list()

    # leio as linhas redirecionadas do stdin
    for line in fileinput.input():

        # Leio a linha e faço o split de seus elementos pelo espaço
        newLine = line.strip().split()

        # Crio o hashmap
        dictCurrentTransaction = { 'timestamp' : int(newLine[0]),
                                    'id' : int(newLine[1]),
                                    'op' : newLine[2],
                                    'at' : newLine[3]}
        # tratamento dos possíveis valores para o ultimo atributo
        # como algumas transações estavam com o mesmo faltante,
        # esta verificação foi incluída
        if len(newLine) == 5:
            if isinstance(newLine[4], int):
                dictCurrentTransaction['newValue'] = int(newLine[4])
            elif isinstance(newLine[4], float):
                dictCurrentTransaction['newValue'] = float(newLine[4])
            else:
               dictCurrentTransaction['newValue'] = newLine[4]
        else:
            dictCurrentTransaction['newValue'] = '-'

        listTransactions.append(dictCurrentTransaction)

        # Confiro se o ID consta na lista, senão adiciono ele 
        if int(newLine[1]) not in listIds:
            listIds.append(int(newLine[1]))

    batchsTransactionsIds = generateListOfBatchs(checkBirthAndDeath(listTransactions, listIds))

    dictVariables = getBatchVariables([listTransactions])

    newBatch = list()

    startIndex = 0

    for batch in batchsTransactionsIds:
        for transaction in listTransactions:
            if transaction['id'] in batch:
                print(transaction)
                newBatch.append(transaction)
        # Neste momento, newBatch contem todas as transações do bloco
        serialityChecking = SerialityTest(newBatch, batch, dictVariables, startIndex)
        serialityChecking.checkConditions()
        startIndex = serialityChecking.currentIndex - 1
        newBatch = []

    print(dictVariables)

