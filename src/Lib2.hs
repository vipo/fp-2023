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
    Condition (..),
    ConditionValue (..),
  )
where

import Data.Char (toLower, isDigit)
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
  | Conditions [Condition]
  deriving (Show, Eq)

data Condition
  = Equals String ConditionValue
  | GreaterThan String ConditionValue
  | LessThan String ConditionValue
  deriving (Show, Eq)

data ConditionValue
  = StrValue String
  | IntValue Int
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
    
parseAggregateFunction :: [String] -> Either ErrorMessage ParsedStatement
parseAggregateFunction statement = parseFunctionBody
  where
    (_, afterWhere) = break (== "where") statement
    (_, afterIs) = break (== "is") afterWhere
    (columnWords, fromAndWhere) = break (== "from") statement
    
    wordsAfterWhere = length afterWhere
    hasWhereClause = wordsAfterWhere > 0
    isBoolIsTrueFalseClauseLike = hasWhereClause && length afterIs == 2
    isAndClauseLike = hasWhereClause && null afterIs

    statementClause :: Either ErrorMessage (Maybe WhereClause)
    statementClause
      | not hasWhereClause && length fromAndWhere == 2 = Right Nothing
      | isBoolIsTrueFalseClauseLike = case parseWhereBoolIsTrueFalse fromAndWhere of
        Left err -> Left err
        Right clause -> Right $ Just clause
      | isAndClauseLike = case parseWhereAnd fromAndWhere of
        Left err -> Left err
        Right clause -> Right $ Just clause
      | otherwise = Left "Unsupported or invalid statement"

    columnName = drop 4 $ init (head columnWords)
    
    tableName
      | length fromAndWhere >= 2 && head fromAndWhere == "from" = fromAndWhere !! 1
      | otherwise = ""

    columnString = unwords columnWords
    columnNames = map (dropWhile (== ' ')) $ splitByComma columnString
  
    parseFunctionBody :: Either ErrorMessage ParsedStatement
    parseFunctionBody = case statementClause of
      Left err -> Left err
      Right clause
        | "avg(" `isPrefixOf` head columnWords && ")" `isSuffixOf` head columnWords && length columnWords == 1 && columnNameExists tableName columnName -> Right (AvgColumn tableName columnName clause)
        | "max(" `isPrefixOf` head columnWords && ")" `isSuffixOf` head columnWords && length columnWords == 1 && columnNameExists tableName columnName -> Right (MaxColumn tableName columnName clause)
        | all (columnNameExists tableName) columnNames -> Right (SelectColumns tableName columnNames clause)
        | otherwise -> Left "Unsupported or invalid statement"

parseWhereBoolIsTrueFalse :: [String] ->Either ErrorMessage WhereClause
parseWhereBoolIsTrueFalse fromAndWhere
  | matchesWhereBoolTrueFalsePatern && isValidWhereClause = splitStatementToWhereClause fromAndWhere
  | otherwise = Left "Unsupported or invalid statement"
  where
    matchesWhereBoolTrueFalsePatern = length fromAndWhere == 6 && head fromAndWhere == "from" && fromAndWhere !! 2 == "where" && fromAndWhere !! 4 == "is" && (fromAndWhere !! 5 == "false" || fromAndWhere !! 5 == "true" )
    tableName = fromAndWhere !! 1
    columnName = fromAndWhere !! 3
    tableColumns = columns (getDataFrameByName tableName)
    columnIsBool = getColumnType (getColumnByName columnName tableColumns) == BoolType
    isValidWhereClause = columnNameExists tableName columnName && columnIsBool

parseWhereAnd :: [String] -> Either ErrorMessage WhereClause
parseWhereAnd fromAndWhere
  | matchesWhereAndPattern (drop 3 fromAndWhere) (fromAndWhere !! 1) = splitStatementToAndClause (drop 3 fromAndWhere) (fromAndWhere !! 1)
  | otherwise = Left "Unsupported or invalid statement"
  where
-- Unsafe
    splitStatementToAndClause :: [String] -> TableName -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList tableName = Right (Conditions (getConditionList strList tableName))
    splitStatementToAndClause _ _ = Left "Unsupported or invalid statement"

-- Unsafe
    getConditionList :: [String] -> TableName -> [Condition]
    getConditionList [condition1, operator, condition2] tableName = [getCondition condition1 operator condition2 tableName]
    getConditionList (condition1 : operator : condition2 : _ : xs) tableName = getCondition condition1 operator condition2 tableName : getConditionList xs tableName

-- Unsafe
getConditionValue :: String -> ConditionValue
getConditionValue condition
  | isNumber condition = IntValue (read condition :: Int)
  | length condition > 2 && "'" `isPrefixOf` condition && "'" `isSuffixOf` condition = StrValue (drop 1 (init condition))

-- Unsafe
getCondition :: String -> String -> String -> String -> Condition
getCondition val1 op val2 tableName
  | val1IsColumn && op == "=" && col1MatchesVal2 = Equals val1 $ getConditionValue val2
  | val2IsColumn && op == "=" && col2MatchesVal1 = Equals val2 $ getConditionValue val1
  | val1IsColumn && op == "<" && col1MatchesVal2 = LessThan val1 $ getConditionValue val2
  | val2IsColumn && op == "<" && col2MatchesVal1 = LessThan val2 $ getConditionValue val1
  | val1IsColumn && op == ">" && col1MatchesVal2 = GreaterThan val1 $ getConditionValue val2
  | val2IsColumn && op == ">" && col2MatchesVal1 = GreaterThan val2 $ getConditionValue val1

  where
    val1IsColumn = columnNameExists tableName val1
    val2IsColumn = columnNameExists tableName val2
    df = getDataFrameByName tableName
    val1Column = getColumnByName val1 (columns df)
    val2Column = getColumnByName val2 (columns df)
    col1MatchesVal2 = compareMaybe (getColumnType val1Column) (parseType val2)
    col2MatchesVal1 = compareMaybe (getColumnType val2Column) (parseType val1)

matchesWhereAndPattern :: [String] -> TableName -> Bool
matchesWhereAndPattern [condition1, operator, condition2] tableName = isWhereAndOperation [condition1, operator, condition2] tableName
matchesWhereAndPattern (condition1 : operator: condition2 : andString: xs) tableName = matchesWhereAndPattern [condition1, operator, condition2] tableName && andString == "and" && matchesWhereAndPattern xs tableName
matchesWhereAndPattern _ _ = False

isWhereAndOperation :: [String] -> TableName -> Bool
isWhereAndOperation [condition1, operator, condition2] tableName
  | columnNameExists tableName condition1  && elem operator [">", "<", "="] && compareMaybe (getColumnType (getColumnByName condition1 (columns (getDataFrameByName tableName)))) (parseType condition2)  = True
  | columnNameExists tableName condition2  && elem operator [">", "<", "="] && compareMaybe (getColumnType (getColumnByName condition2 (columns (getDataFrameByName tableName)))) (parseType condition1)  = True
  | otherwise = False
isWhereAndOperation _ _ = False

parseType :: String -> Maybe ColumnType
parseType str
  | isNumber str = Just IntegerType
  | "'" `isPrefixOf` str && "'" `isSuffixOf` str = Just StringType
  | otherwise = Nothing


splitStatementToWhereClause :: [String] -> Either ErrorMessage WhereClause
splitStatementToWhereClause ["from", tableName, "where", boolColName, "is", boolString] = Right $ IsValueBool parsedBoolString tableName boolColName
  where
    parsedBoolString = boolString == "true"
splitStatementToWhereClause _ = Left "Unsupported or invalid statement"


splitByComma :: String -> [String]
splitByComma = map (dropWhile (== ' ')) . words . map (\c -> if c == ',' then ' ' else c)

parseSemiCaseSensitive :: String -> [String]
parseSemiCaseSensitive statement = convertedStatement
  where
    splitStatement = words statement
    convertedStatement = map wordToLowerSensitive splitStatement

wordToLowerSensitive :: String -> String
wordToLowerSensitive word
  | map toLower word `elem` keywords = map toLower word
  | "avg(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "avg(" ++ drop 4 (init word) ++ ")"
  | "max(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "max(" ++ drop 4 (init word) ++ ")"
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
      case mapM (`findColumnIndex` df) columnNames of
        Just columnIndices -> selectColumnsFromDataFrame whereCondition tableName columnIndices
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
  | isTableInDatabase name && col `elem` getColNameList (columns (getDataFrameByName name)) && getColumnType (getColumnByName col (columns (getDataFrameByName name))) == BoolType = Right $ getRowsByBool bool (getDataFrameRows (getDataFrameByName name))
  | otherwise = Left "Dataframe does not exist or does not contain column by specified name or column is not of type bool"
  where
    getRowsByBool :: Bool -> [Row] -> DataFrame
    getRowsByBool boolValue tableRows = DataFrame (columns (getDataFrameByName name)) (filter (\row -> rowCellAtIndexIsBool boolValue row $ elemIndex col (getColNameList (columns (getDataFrameByName name)))) tableRows)

    rowCellAtIndexIsBool :: Bool -> Row -> Maybe Int -> Bool
    rowCellAtIndexIsBool boolVal row index = case index of
      Just ind -> row !! ind == BoolValue boolVal
      Nothing -> False
--selectColumns
selectColumnsFromDataFrame :: Maybe WhereClause -> TableName -> [Int] -> Either ErrorMessage DataFrame
selectColumnsFromDataFrame whereCondition tableName columnIndices = do
    let realCols = columns (executeWhere whereCondition tableName)
        realRows = getDataFrameRows (executeWhere whereCondition tableName)
        selectedColumns = map (realCols !!) columnIndices
        selectedRows = map (\row -> map (row !!) columnIndices) realRows
    Right $ DataFrame selectedColumns selectedRows

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
  | col `elem` getColNameList cols && isRightValue (getColumnByName col cols) = Right (maximum'' columnValues)
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

isNumber :: String -> Bool
isNumber "" = False
isNumber xs =
  case dropWhile isDigit xs of
    "" -> True
    _ -> False

compareMaybe :: Eq a => a -> Maybe a -> Bool
compareMaybe val1 (Just val2) = val1 == val2
compareMaybe _ Nothing = False
