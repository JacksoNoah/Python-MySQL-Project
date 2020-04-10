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

    my_cursor = mydb.cursor(buffered=True)

    # creating relations inside of mySQL workbench
    # my_cursor.execute("CREATE DATABASE Homework5")
    my_cursor.execute("SHOW DATABASES")
    # my_cursor.execute("CREATE TABLE GenreRelationship (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY, genreID Integer(10), " "name VARCHAR(255))")
    my_cursor.execute("CREATE TABLE GenreRelationship (id INTEGER AUTO_INCREMENT PRIMARY KEY , genreID Integer(10), " "genreName VARCHAR(255))")

    # my_cursor.execute("CREATE TABLE Genre (id INTEGER(10) PRIMARY KEY, name VARCHAR(255))")
    # my_cursor.execute("SHOW TABLES") my_cursor.execute("CREATE TABLE GenreRelationship (id INT AUTO_INCREMENT
    # PRIMARY KEY, name VARCHAR(255), genre_id INTEGER(10))")
    #
    # my_cursor.execute("CREATE TABLE Keyword (id INTEGER(
    # 10), name VARCHAR(255))") my_cursor.execute("CREATE TABLE Production_Companies (id INTEGER(10), name VARCHAR(
    # 255))")

    # my_cursor.execute("CREATE TABLE Production_Countries (iso_3166_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")

    fill_relations(mydb, my_cursor, df)

    return


# Fills relations with data provided
def fill_relations(mydb, my_cursor, df):
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

    dfArray = [genreDF, keywordDF, prodCompaniesDF, prodCountriesDF, spokenLangDF]

    get_data(df, genreDF, 'genres', df['id'])
    # get_data(df, keywordDF, 'keywords', df['id'])
    # get_data(df, prodCompaniesDF, 'production_companies', df['id'])
    #  get_data(df, prodCountriesDF, 'production_countries', df['id'])
    #  get_data(df, spokenLangDF, 'spoken_languages', df['id'])

    genreDF.dropna(how='all', inplace=True)
    keywordDF.dropna(how='all', inplace=True)
    prodCompaniesDF.dropna(how='all', inplace=True)
    prodCountriesDF.dropna(how='all', inplace=True)
    spokenLangDF.dropna(how='all', inplace=True)

    i = 0

    # print(genreDF)
    # print(keywordDF)
    # print(prodCompaniesDF)
    # print(prodCountriesDF)
    # print(spokenLangDF)
    tableName = "GenreRelationship"

    # adding genre data to mysql
    sqlFormulaGenreRel = "INSERT INTO GenreRelationship (id, genreID, genreName) VALUES ( %s, %s, %s)"
    i = 1
    while i < genreDF.shape[0]:
        genreTuple = (i, genreDF.genreID.iloc[i], genreDF.genreName.iloc[i])
        my_cursor.execute(sqlFormulaGenreRel, genreTuple)
        i += 1

    ''' genreUniqLen = len(genreDF.genreName.unique())
    sqlFormulaGenre = "INSERT INTO Genre ( id, name) VALUES ( %s, %s)"
    # genreDF.to_sql(con=mydb, name='Genre', if_exists='replace')
    i = 0
    while i < genreUniqLen:
        genreTuple = (genreDF.genreID.unique()[i], genreDF.genreName.unique()[i])
        my_cursor.execute(sqlFormulaGenre, genreTuple)
        i += 1
    '''


    mydb.commit()
    # inserting into Genre relation, which holds genre id and genre name
    mydb.close()

    return


# parses/extracts data of a specific attribute, "attr", of the DataFrame "df"
def get_data(df, newDF, attr, movieIds):
    # get column names of data frame being inserted into
    colOneName = newDF.columns[1]
    colTwoName = newDF.columns[2]

    count = 0  # used to keep track of indexes when adding data to newDF

    i = 0
    while i < df.shape[0]:

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
        j = 0
        # while j is less than length of row

        while j < randLength and count < (df.shape[0] - 1):

            # char is comma, and comma count is odd, set id attribute in newDF
            if randData[j] == "," and (commaCount % 2) == 1:
                # set row values of id column, and movieID column
                newDF['movieID'].iloc[count] = movieIds[count]
                newDF[colOneName].iloc[count] = randDataId
                commaCount += 1
                randDataId = ""
                j += 1

            # if char is a space then we have reached end of word then set name attribute in newDF
            elif randData[j] == "," and (commaCount % 2) == 0:
                # making sure randDataName isnt just space characters
                if not randDataName.isspace() and randDataName != "":
                    # set row values of name column, and movieID column
                    newDF['movieID'].iloc[count] = movieIds[count]
                    newDF[colTwoName].iloc[count] = randDataName

                commaCount += 1
                count += 1
                j += 1

                randDataName = ""  # used to store name attribute from data row i

            # if char is first space reached in row its id attributes data
            elif randData[j] != "," and randData != " ":

                # we know characters are now id data
                if (commaCount % 2) == 0:
                    randDataId += randData[j]

                # we know characters are now name data
                if (commaCount % 2) == 1:
                    randDataName += randData[j]

            j += 1

        i += 1
    return


def main():
    create_relations()


if __name__ == "__main__":
    main()
