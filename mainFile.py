##
## Noah Jackson
## CS351 -- Homework 5
##

import mysql.connector
import pandas as pd
import datetime


# creates relations in mySQL workbench for the supplied data
def create_relations():
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='Tricky123!',
        database="Homework5"
    )

    print(mydb)

    df = pd.read_csv('/home/noahj734/Desktop/PycharmProjects/Databases/tmdb_5000_movies.csv')
    print(df.shape)

    # print(df.columns.values)     # example how to print column names

    my_cursor = mydb.cursor()

    # creating relations inside of mySQL workbench
    # my_cursor.execute("CREATE DATABASE Homework5")
    # my_cursor.execute("SHOW DATABASES")
    # my_cursor.execute("SHOW TABLES")
    # my_cursor.execute("CREATE TABLE Genre (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Keyword (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Production_Companies (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Production_Countries (iso_3166_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")

    fill_relations(mydb, df)

    return


# Fills relations with data provided
def fill_relations(mydb, df):
    indexArray = []
    arrayCount = 0
    while arrayCount < (df.shape[0]):
        indexArray.append(arrayCount)
        arrayCount += 1

    # setting up all data frames for data parsing/extraction
    genreColumns = ['movieID', 'genreID', 'genreName']
    genreDF = pd.DataFrame(index=indexArray, columns=genreColumns)

    keywordColumns = ['movieID', 'keywordID', 'keywordName']
    keywordDF = pd.DataFrame(index=indexArray, columns=keywordColumns)

    prodCompaniesColumns = ['movieID', 'prodCompanyID', 'prodCompanyName']
    prodCompaniesDF = pd.DataFrame(index=indexArray, columns=prodCompaniesColumns)

    prodCountriesColumns = ['movieID', 'prodCountryID', 'prodCountryName']
    prodCountriesDF = pd.DataFrame(index=indexArray, columns=prodCountriesColumns)

    spokenLangColumns = ['movieID', 'spokenLangID', 'spokenLangName']
    spokenLangDF = pd.DataFrame(index=indexArray, columns=spokenLangColumns)

    dfArray = []
    dfArray.append(genreDF)
    dfArray.append(keywordDF)
    dfArray.append(prodCompaniesDF)
    dfArray.append(prodCountriesDF)
    dfArray.append(spokenLangDF)

    get_data(df, genreDF, 'genres', df['id'])
    get_data(df, keywordDF, 'keywords', df['id'])
    get_data(df, prodCompaniesDF, 'production_companies', df['id'])
    get_data(df, prodCountriesDF, 'production_countries', df['id'])
    get_data(df, spokenLangDF, 'spoken_languages', df['id'])

    genreDF.dropna(how='all', inplace=True)
    keywordDF.dropna(how='all', inplace=True)
    prodCompaniesDF.dropna(how='all', inplace=True)
    prodCountriesDF.dropna(how='all', inplace=True)
    spokenLangDF.dropna(how='all', inplace=True)

    print(genreDF)
    print(keywordDF)
    print(prodCompaniesDF)
    print(prodCountriesDF)
    print(spokenLangDF)

    sqlFormula = "INSERT INTO Genre ( id, name) VALUES ( %s, %s)"

    return




# parses/extracts data of a specific attribute, "attr", of the DataFrame "df"
def get_data(df, newDF, attr, movieIds):
    # get column names of data frame being inserted into
    colOneName = newDF.columns[1]
    colTwoName = newDF.columns[2]
    count = 0  # used to keep track of indexes when adding data to newDF
    i = 0
    while i < df.shape[0]:

        evenOddFlag = 0  # is 0 when char is odd numbered space, and 1 when char is even space in row
        dataFlag = 0  # if 1 then data element is id, or newDF.columns[1], if 2 then newDF.columns[2]
        commaCount = 0  # keep track of which space

        randDataName = ""  # used to store name attribute from data row i
        randDataId = ""  # used to store id attribute from data row i

        # get data from row i
        randData = df[attr].iloc[i]

        # remove all chars in '(){}<>":[]' from the rows data to help with parsing
        randData = ''.join(c for c in randData if c not in '{}[]<>":')
        randData = randData.replace("id", "")
        randData = randData.replace("name", "")
        randData = randData.replace("iso_3166_1", "")
        randData = randData.replace("iso_639_1", "")

        # print("randData: %s\n" % randData)
        randLength = len(randData)

        commaCount = 0
        j = 1
        # while j is less than length of row

        while j < randLength and count < (df.shape[0] - 1):
            # if char is a space then we have reached end of word then set id attribute in newDF
            if randData[j] == "," and (commaCount % 2) == 1:
                # print("id: %s\n" % randDataId)
                newDF['movieID'].iloc[count] = movieIds[count]
                newDF[colOneName].iloc[count] = randDataId
                commaCount += 1
                randDataId = ""  # used to store id attribute from data row i
                evenOddFlag = 0
                j += 1

            # if char is a space then we have reached end of word then set name attribute in newDF
            elif randData[j] == "," and (commaCount % 2) == 0:

                if not randDataName.isspace() and randDataName != "":
                    newDF['movieID'].iloc[count] = movieIds[count]
                    # print("name: %s\n" % randDataName)
                    newDF[colTwoName].iloc[count] = randDataName
                commaCount += 1
                count += 1
                evenOddFlag = 1
                j += 1

                randDataName = ""  # used to store name attribute from data row i

            # if char is first space reached in row its id attributes data
            elif randData[j] != "," and randData != " ":

                # we know characters are now id data
                if evenOddFlag == 0:
                    randDataId += randData[j]

                # we know characters are now name data
                if evenOddFlag == 1:
                    randDataName += randData[j]

            j += 1

        # print("randDataId: %s\nrandDataName: %s\n" % (randDataId, randDataName))

        i += 1
    # dat.dropna(subset=[col_list])
    #newDF.dropna(subset=['movieID', colOneName, colTwoName])
    return
    # print("i: %d\n" % i)


def main():
    create_relations()


if __name__ == "__main__":
    main()
