{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    filterRowsByBoolColumn,
    sqlMax,
    WhereClause (..),
    ParsedStatement (..),
  )
where

import Data.Char (toLower)
import Data.List (elemIndex, find, isPrefixOf, isSuffixOf)
import Data.Maybe (fromMaybe)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables (TableName, database)

type ErrorMessage = String

data ParsedStatement
  = ShowTable TableName
  | ShowTables
  | AvgColumn TableName String (Maybe WhereClause)
  | SelectColumns TableName [String] (Maybe WhereClause)
  | MaxColumn TableName String (Maybe WhereClause)
  deriving (Show, Eq)

data WhereClause
  = IsValueBool Bool TableName String
  deriving (Show, Eq)

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | last input /= ';' = Left "Unsupported or invalid statement"
  | otherwise = mapStatementType wordsInput
  where
    cleanedInput = init input
    wordsInput = parseSemiCaseSensitive cleanedInput

mapStatementType :: [String] -> Either ErrorMessage ParsedStatement
mapStatementType statement = case statement of
  ["show", "table", tableName] ->
    if tableNameExists tableName
      then Right (ShowTable tableName)
      else Left "Table not found"
  ["show", "tables"] -> Right ShowTables
  "select" : rest -> parseAggregateFunction rest
  _ -> Left "Unsupported or invalid statement"

parseFromAndWhere :: [String] -> ([String], [String])
parseFromAndWhere fromAndWhere
  | length fromAndWhere == 6 && head fromAndWhere == "from" && fromAndWhere !! 2 == "where" && fromAndWhere !! 4 == "is" = break (== "where") fromAndWhere
  | length fromAndWhere == 2 && head fromAndWhere == "from" = (["from", fromAndWhere !! 1], ["where", "", "is", ""])
  | otherwise = (["from", ""], ["where", "", "is", ""])
    
parseAggregateFunction :: [String] -> Either ErrorMessage ParsedStatement
parseAggregateFunction statement = parseFunctionBody
  where
    (columnWords, fromAndWhere) = break (== "from") statement
    (["from", tableName], ["where", boolColName, "is", boolString]) = parseFromAndWhere fromAndWhere
      
    boolStringIsValid = boolString == "true" || boolString == "false" || boolString == ""
    parsedBoolString = boolString == "true"
    whereFilter =
      if boolStringIsValid && columnNameExists tableName boolColName && getColumnType (getColumnByName boolColName (columns (getDataFrameByName tableName))) == BoolType
        then Just (IsValueBool parsedBoolString tableName boolColName)
        else Nothing
    columnName = drop 4 $ init (head columnWords)
    tableAndColumnExists = tableNameExists tableName && columnNameExists tableName columnName
    columnString = unwords columnWords
    columnNames = map (dropWhile (== ' ')) $ splitByComma columnString
  
    parseFunctionBody :: Either ErrorMessage ParsedStatement
    parseFunctionBody
      | not boolStringIsValid && columnNameExists tableName boolColName || boolStringIsValid && (boolString /= "") && not (columnNameExists tableName boolColName) = Left "Unsupported or invalid statement"
      | "avg(" isPrefixOf head columnWords && ")" isSuffixOf head columnWords && tableAndColumnExists && length columnWords == 1 = Right (AvgColumn tableName columnName whereFilter)
      | "max(" isPrefixOf head columnWords && ")" isSuffixOf head columnWords && tableAndColumnExists && length columnWords == 1 = Right (MaxColumn tableName columnName whereFilter)
      | tableNameExists tableName && all (columnNameExists tableName) columnNames = Right (SelectColumns tableName columnNames whereFilter)
      | otherwise = Left "Unsupported or invalid statement"

splitByComma :: String -> [String]
splitByComma = map (dropWhile (== ' ')) . words . map (\c -> if c == ',' then ' ' else c)

parseSemiCaseSensitive :: String -> [String]
parseSemiCaseSensitive statement = convertedStatement
  where
    splitStatement = words statement
    convertedStatement = map wordToLowerSensitive splitStatement

wordToLowerSensitive :: String -> String
wordToLowerSensitive word
  | map toLower word elem keywords = map toLower word
  | "avg(" isPrefixOf map toLower word && ")" isSuffixOf word = "avg(" ++ drop 4 (init word) ++ ")"
  | "max(" isPrefixOf map toLower word && ")" isSuffixOf word = "max(" ++ drop 4 (init word) ++ ")"
  | otherwise = word
  where
    keywords = ["select", "from", "where", "show", "table", "tables", "false", "true", "and", "is"]

tableNameExists :: TableName -> Bool
tableNameExists name = any (\(tableName, _) -> tableName == name) database

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable tableName) =
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)
executeStatement (AvgColumn tableName columnName whereCondition) =
  case lookup tableName database of
    Just df@(DataFrame _ _) ->
      case findColumnIndex columnName df of
        Just columnIndex ->
          let values = map (\row -> getColumnValue columnIndex row) (getDataFrameRows (executeWhere whereCondition tableName))
              validIntValues = filter isIntegerValue values
           in if null validIntValues
                then Left "No valid integers found in the specified column"
                else
                 averageOfIntValues validIntValues
        Nothing -> Left $ "Column " ++ columnName ++ " not found in table " ++ tableName
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement (SelectColumns tableName columnNames whereCondition) =
  case lookup tableName database of
    Just df ->
      case mapM (findColumnIndex df) columnNames of
        Just columnIndices ->
          let realCols = columns (executeWhere whereCondition tableName)
              realRows = getDataFrameRows (executeWhere whereCondition tableName)
              selectedColumns = map (realCols !!) columnIndices
              selectedRows = map (\row -> map (row !!) columnIndices) realRows
           in Right $ DataFrame selectedColumns selectedRows
        Nothing -> Left $ "One or more columns not found in table " ++ tableName
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement (MaxColumn tableName columnName whereCondition) = case sqlMax (executeWhere whereCondition tableName) columnName of
  Right value -> Right (DataFrame [getColumnByName columnName (columns (getDataFrameByName tableName))] [[value]])
  Left msg -> Left msg

executeWhere :: Maybe WhereClause -> TableName -> DataFrame
executeWhere whereClause tableName = case whereClause of
  Just (IsValueBool bool table column) -> case filterRowsByBoolColumn table column bool of
    Right df -> df
    Left _ -> getDataFrameByName tableName
  Nothing -> getDataFrameByName tableName

-- Filter rows based on whether the specified column's value is TRUE or FALSE.
filterRowsByBoolColumn :: TableName -> String -> Bool -> Either ErrorMessage DataFrame
filterRowsByBoolColumn name col bool
  | isTableInDatabase name && col elem getColNameList (columns (getDataFrameByName name)) && getColumnType (getColumnByName col (columns (getDataFrameByName name))) == BoolType = Right $ getRowsByBool bool (getDataFrameRows (getDataFrameByName name))
  | otherwise = Left "Dataframe does not exist or does not contain column by specified name or column is not of type bool"
  where
    getRowsByBool :: Bool -> [Row] -> DataFrame
    getRowsByBool boolValue tableRows = DataFrame (columns (getDataFrameByName name)) (filter (\row -> rowCellAtIndexIsBool boolValue row $ elemIndex col (getColNameList (columns (getDataFrameByName name)))) tableRows)

    rowCellAtIndexIsBool :: Bool -> Row -> Maybe Int -> Bool
    rowCellAtIndexIsBool boolVal row index = case index of
      Just ind -> row !! ind == BoolValue boolVal
      Nothing -> False
      
--AVG agregate function
averageOfIntValues :: [Value] -> Either ErrorMessage DataFrame
averageOfIntValues validIntValues 
  | null validIntValues = Left "No valid integers found in the specified column"
  | otherwise =
      let sumValues = sumIntValues validIntValues
          avg = fromIntegral sumValues / fromIntegral (length validIntValues)
      in Right $ DataFrame [Column "AVG" IntegerType] [[IntegerValue (round avg)]]

-- max aggregate function
sqlMax :: DataFrame -> String -> Either ErrorMessage Value
sqlMax df col
  | col elem getColNameList cols && isRightValue (getColumnByName col cols) = Right (maximum'' columnValues)
  | otherwise = Left "Cannot get max of this value type or table does not exist"
  where
    cols = columns df
    columnValues = getValues (getDataFrameRows df) (fromMaybe 0 (elemIndex col (getColNameList (columns df))))

    isRightValue :: Column -> Bool
    isRightValue (Column _ valueType) = valueType == IntegerType || valueType == StringType || valueType == BoolType

    getValues :: [Row] -> Int -> [Value]
    getValues rows index = map (!! index) rows

    maximum'' :: [Value] -> Value
    maximum'' [x] = x
    maximum'' (x : x' : xs) = maximum'' ((if compValue x x' then x else x') : xs)

    compValue :: Value -> Value -> Bool
    compValue (IntegerValue val1) (IntegerValue val2) = val1 > val2
    compValue (StringValue val1) (StringValue val2) = val1 > val2
    compValue (BoolValue val1) (BoolValue val2) = val1 > val2
    compValue (IntegerValue _) NullValue = True
    compValue (StringValue _) NullValue = True
    compValue (BoolValue _) NullValue = True
    compValue NullValue (IntegerValue _) = False
    compValue NullValue (StringValue _) = False
    compValue NullValue (BoolValue _) = False
    compValue _ _ = True

-- Util functions
columnNameExists :: TableName -> String -> Bool
columnNameExists tableName columnName =
  case lookup tableName database of
    Just df ->
      any (\(Column name _) -> name == columnName) (columns df)
    Nothing -> False

getColumnByName :: String -> [Column] -> Column
getColumnByName name cols = fromMaybe (Column "notfound" BoolType) (find (\(Column colName _) -> colName == name) cols)

getColNameList :: [Column] -> [String]
getColNameList = map (\(Column name _) -> name)

getColumnType :: Column -> ColumnType
getColumnType (Column _ columnType) = columnType

getDataFrameByName :: TableName -> DataFrame
getDataFrameByName name = fromMaybe (DataFrame [] []) (lookup name database)

getDataFrameRows :: DataFrame -> [Row]
getDataFrameRows (DataFrame _ rows) = rows

isTableInDatabase :: TableName -> Bool
isTableInDatabase name = case lookup name database of
  Just _ -> True
  Nothing -> False

findColumnIndex :: String -> DataFrame -> Maybe Int
findColumnIndex columnName (DataFrame cols _) =
  elemIndex columnName (map (\(Column name _) -> name) cols)

getColumnValue :: Int -> Row -> Value
getColumnValue columnIndex row = row !! columnIndex

isIntegerValue :: Value -> Bool
isIntegerValue (IntegerValue _) = True
isIntegerValue _ = False

sumIntValues :: [Value] -> Integer
sumIntValues = foldr (\(IntegerValue x) acc -> x + acc) 0

isNullValue :: Value -> Bool
isNullValue NullValue = True
isNullValue _ = False


columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols