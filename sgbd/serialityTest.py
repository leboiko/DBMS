from sgbd import makeJournal as mj

class SerialityTest:
    """
        Esta classe é responsável por executar os testes de seriabilidade em um pacote de transações
        e interage com a classe 'MakeJournal'
    """
    def __init__(self, batchOfTransactions, idsInBlock, dictVariableValues, currentIndex=0):
        """
            Construtor de um novo objeto 'SerialityTest'

            :batchOfTransactions: Batch de transações para testar conflito de seriabilidade.
            :idsInBlock: Identificadores das transações no bloco.
            :dictPossibleAttr: Um dicionário com os atributos ativos no bloco e seu respectivo valor.
            :currentIndex: Indice inicial para os registros de log. Caso não seja inicializado o valor default 0 é utilizado.
        """
        self.batchOfTransactions = batchOfTransactions
        self.idsInBlock = idsInBlock
        self.dictVariableValues = dictVariableValues
        self.currentIndex = currentIndex

    
    def checkConditions(self):
        """
            Esta função é chamada externamente para gerar os registros de log. 
            Ao final, o currentIndex é atualizado.
        """

        graph = self.searchOperations()
        hasCycle = self.cycle_exists(graph)
        newJournal = mj.MakeJournal(
            self.batchOfTransactions, 
            hasCycle, 
            self.idsInBlock,
            self.dictVariableValues,
            self.currentIndex)
        newJournal.generateLog()
        self.currentIndex = newJournal.currentIndex()


    def searchOperations(self):
        """
            Esta função cria um grafo no formato de um dicionário, como o exemplo:

            graph = { 1 : [],
                      2 : [],
                      3 : []
                    } 

            e chama a função searchOperations para criar arestas de acordo com os seguintes critérios
            do teste de seriabilidade por conflito:

            Aresta Ti -> Tj para cada r(x) em Tj depois de w(x) em Ti 
            Aresta Ti -> Tj para cada w(x) em Tj depois de r(x) em Ti
            Aresta Ti -> Tj para cada w(x) em Tj depois de w(x) em Ti

            ao final é retornado um grafo como o exemplo a seguir

            graph = { 1 : [2],
                      2 : [1],
                      3 : [1, 2]
                    } 

        """
        graph = self.createEmptyGraph()
        indexInList = 0
        listOperations = [['w', 'r'], ['r', 'w'], ['w', 'w']]

        for firstOp, secondOp in listOperations:
            for transaction in self.batchOfTransactions:
                    if transaction['op'].upper() == firstOp.upper():
                        for i in range(indexInList, len(self.batchOfTransactions)):
                            if ((self.batchOfTransactions[i]['op'].upper() == secondOp.upper()) and 
                            (self.batchOfTransactions[i]['at'] == transaction['at']) and 
                            (self.batchOfTransactions[i]['id'] != transaction['id'])):
                                if self.batchOfTransactions[i]['id'] not in graph[transaction['id']]:
                                    graph[transaction['id']].append(self.batchOfTransactions[i]['id'])
                    indexInList += 1
            indexInList = 0

        return graph

    def createEmptyGraph(self):
        """
            Esta função cria um grafo vazio, como o exemplo a seguir:

            graph = { 1 : [],
                      2 : [],
                      3 : []
                    } 

            O grafo é construido no formato de um dicionário, de acordo com 
            os ID'S presentes na lista idsInBloc.

            Ao final, a função retorna o grafo.

        """
        graph = dict()
        for id in self.idsInBlock:
            graph[id] = []
            
        return graph

    def cycle_exists(self, G):                     
        """
            Esta função recebe o seguinte parâmetro:

            :G: Grafo direcional na forma de um dicionário

            A função inicia setando todos os nodos do grafo para a cor "white".
            Após, criamos o atributo found_cycle em um formato de lista, para que possa
            ser passado por referência.

            Após a função dfs_visit é chamada para cada um for nodos presentes no grafo
            que possuírem cor "white".

            Ao final, a função retorna True caso tenha encontrado ciclo no grafo ou False caso contrário

        """
        color = { u : "white" for u in G  } 
        found_cycle = [False]
        for u in G:                     
            if color[u] == "white":
                self.dfs_visit(G, u, color, found_cycle)
            if found_cycle[0]:
                break

        return found_cycle[0]
 
    def dfs_visit(self, G, u, color, found_cycle):
        """
            Esta função recebe os seguintes parâmetros:

            :G: Grafo direcional na forma de um dicionário
            :u: inteiro referente ao vértice a ser analisado
            :color: string referente a cor ("white", "gray", "black")
            :found_cycle: Lista recebida por referência contendo apenas uma posição com um valor Bool

            A função inicia verificando se existe ciclo. Caso exista, ela para.

            Após é setado o vértice para a cor "gray" (visitado) e procura os vizinhos aonde G[u] é a 
            lista de adjacencias de u. Em seguida, é verificado se existe um loop (cor "gray") iniciado pelo vértice analisado,
            setando a variavel found_cycle para True caso exista. Caso o vértice seja da cor "white", ou seja, ainda
            nao foi visitado, a função dfs_visit é chamada recursivamente.

            Ao final, o vértice analisado é setado para a cor "black", para que não seja mais utilizado para busca.

        """
        if found_cycle[0]:                          
            return
        color[u] = "gray"                           
        for v in G[u]:                              
            if color[v] == "gray":                    
                found_cycle[0] = True       
                return
            if color[v] == "white":                  
                self.dfs_visit(G, v, color, found_cycle)
        color[u] = "black"                          