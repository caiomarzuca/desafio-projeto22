import pyspark as sp
import os

config = sp.SparkConf().setAppName("indexing").setMaster("local[*]")
sc = sp.SparkContext(conf=config)

indice_dataset = 0  # Índice para percorrer o diretório dataset 'indice_dataset'
cont = 0  # Contador para auxiliar percorrer diretorio com arquivos
dictionary = {}
wordId_docId = []
wordId = 0
docId = 0

while cont < len(os.listdir("dataset")):
    caminho_arquivo = "dataset/" + str(indice_dataset)
    docs = sc.textFile(caminho_arquivo)
    word_list = docs.flatMap(lambda doc: doc.split())

    for word in word_list.collect():
        if word in dictionary.values():
            # Operacao para a variável x:
            #     separa os valores do dicionário em uma lista, localiza a posição do
            #     valor que você possui e obtém a chave nessa posição
            x = list(dictionary.keys())[list(dictionary.values()).index(word)]
            wordId_docId.append((x, docId))
            continue
        else:
            dictionary[wordId] = word
            wordId_docId.append((wordId, docId))
            wordId += 1

    docId += 1
    indice_dataset += 1
    cont += 1

# Algoritimo para criação do INDICE REVERSO
wordId = 0
docId = 0
cont = 0
indexing_list = []
docId_list = []

while cont < len(list(dictionary.keys())):
    for tupla in wordId_docId:
        if list(dictionary.keys())[cont] == str(tupla.__getitem__(0)):
            docId_list.append(tupla.__getitem__(1))
    indexing_list.append((list(dictionary.keys())[cont], sorted(set(docId_list))))
    docId_list = []
    cont += 1

# Salvando em arquivo .txt o dicionário, índice reverso e tuplas wordID e docID
wordId_docId_file = open("wordId_docId.txt", "a", encoding="utf-8")
wordId_docId_file.writelines(str(wordId_docId))

dictionary_file = open("Dictionary.txt", "a", encoding="utf-8")
dictionary_file.writelines(str(dictionary))

indexing_list_file = open("Indexing_reverse.txt", "a", encoding="utf-8")
indexing_list_file.writelines(str(indexing_list))
