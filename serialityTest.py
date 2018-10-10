from graph import cycle_exists
from makeJournal import MakeJournal

class SerialityTest:

    def __init__(self, batchOfTransactions, idsInBlock, dictVariableValues, currentIndex=0):
        self.batchOfTransactions = batchOfTransactions
        self.idsInBlock = idsInBlock
        self.dictVariableValues = dictVariableValues
        self.currentIndex = currentIndex

    # Esta função recebe o batch de transações e chama a função searchOperations
    # passando para ela como parametro o batch de transações e as condições 
    # para observar no teste de serialidade por conflito
    def checkConditions(self):
        # Crio um dicionario e populo com as chaves sendo os Ids do bloco
        # e os valores associados listas vazias
        graph = self.createEmptyGraph()
        # Aresta Ti -> Tj para cada r(x) em Tj depois de w(x) em Ti 
        graph = self.searchOperations('w', 'r', graph)
        # Aresta Ti -> Tj para cada w(x) em Tj depois de r(x) em Ti
        graph = self.searchOperations('r', 'w', graph)
        # Aresta Ti -> Tj para cada w(x) em Tj depois de w(x) em Ti
        graph = self.searchOperations('w', 'w', graph)
        hasCycle = cycle_exists(graph)
        newJournal = MakeJournal(
            self.batchOfTransactions, 
            hasCycle, 
            self.idsInBlock,
            self.dictVariableValues,
            self.currentIndex)
        newJournal.generateLog()

        self.currentIndex = newJournal.currentIndex()

    # Esta função recebe o batch de transações, a primeira operação a ser considerada, tal 
    # como a segunda, e retorna true caso a condição seja satisfeita e falso caso contrário
    def searchOperations(self, firstOp, secondOp, graph):
        indexInList = 0
        for transaction in self.batchOfTransactions:
                if (transaction['op'] == firstOp.upper()) or (transaction['op'] == firstOp.lower()):
                    for i in range(indexInList, len(self.batchOfTransactions)):
                        if (((self.batchOfTransactions[i]['op'] == secondOp.upper()) or 
                        (self.batchOfTransactions[i]['op'] == secondOp.lower())) and 
                        (self.batchOfTransactions[i]['at'] == transaction['at']) and 
                        (self.batchOfTransactions[i]['id'] != transaction['id'])):
                            if self.batchOfTransactions[i]['id'] not in graph[transaction['id']]:
                                graph[transaction['id']].append(self.batchOfTransactions[i]['id'])
                indexInList += 1

        return graph

    # Esta função cria um dicionario sendo que as chaves são os vértices
    # e os valores representam os vertices conectados
    def createEmptyGraph(self):
        graph = dict()
        for id in self.idsInBlock:
            graph[id] = []
            
        return graph