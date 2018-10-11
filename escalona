#!/usr/bin/env python3
# from sgbd import serialityTest as st
from sgbd import splitTransactions as st
import fileinput
import os


def deletePreviousLog(fileName):
    if os.path.isfile(fileName):
        os.remove(fileName)

if __name__ == '__main__':
    deletePreviousLog('archive.log')
    listTransactions = list()
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
    

    a = st.SplitTransactions(listTransactions, listIds)
    a.split()
    dictVariables = a.getDictVariables()

    # Imprimo os resultados
    orderedListOfKeys = sorted(list(dictVariables))
    for key in orderedListOfKeys:
        print('{0};{1}'.format(key.upper(),dictVariables[key]))

