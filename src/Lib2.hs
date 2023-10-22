{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    filterRowsByBoolColumn,
    sqlMax,
    ParsedStatement (..),
  )
where

import Data.Char (toLower)
import Data.Maybe (fromMaybe)
import Data.List (elemIndex, isPrefixOf, isSuffixOf, find)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables (TableName, database)

type ErrorMessage = String

data ParsedStatement
  = ShowTable TableName
  | ShowTables
  | AvgColumn TableName String
  deriving (Show, Eq)

columnNameExists :: TableName -> String -> Bool
columnNameExists tableName columnName =
  case lookup tableName database of
    Just df ->
      any (\(Column name _) -> name == columnName) (columns df)
    Nothing -> False

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | last input /= ';' = Left "Unsupported or invalid statement"
  | otherwise =
      let cleanedInput = init input
          wordsInput = parseSemiCaseSensitive cleanedInput
       in case wordsInput of
            ["show", "table", tableName] ->
              if tableNameExists tableName
                then Right $ ShowTable tableName
                else Left "Table not found"
            ["show", "tables"] -> Right ShowTables
            "select" : rest ->
              case rest of
                func : "from" : tableName : []
                  | "avg(" `isPrefixOf` func && ")" `isSuffixOf` func ->
                      let columnName = drop 4 $ init func
                       in if tableNameExists tableName && columnNameExists tableName columnName
                            then Right $ AvgColumn tableName columnName
                            else Left "Unsupported or invalid statement"
                _ -> Left "Unsupported or invalid statement"
            _ -> Left "Unsupported or invalid statement"

parseSemiCaseSensitive :: String -> [String]
parseSemiCaseSensitive statement = convertedStatement
  where
    keywords = ["select", "from", "where", "show", "table", "tables"]
    splitStatement = words statement
    convertedStatement = map wordToLowerSensitive splitStatement

    wordToLowerSensitive :: String -> String
    wordToLowerSensitive word
      | map toLower word `elem` keywords = map toLower word
      | "avg(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "avg(" ++ drop 4 (init word) ++ ")"
      | otherwise = word

tableNameExists :: TableName -> Bool
tableNameExists name = any (\(tableName, _) -> tableName == name) database

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable tableName) =
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)
executeStatement (AvgColumn tableName columnName) =
  case lookup tableName database of
    Just df@(DataFrame cols rows) ->
      case findColumnIndex columnName df of
        Just columnIndex ->
          let values = map (\row -> getColumnValue columnIndex row) rows
              validIntValues = filter isIntegerValue values
           in if null validIntValues
                then Left "No valid integers found in the specified column"
                else
                  let sumValues = sumIntValues validIntValues
                      avg = fromIntegral sumValues / fromIntegral (length validIntValues)
                   in Right $ DataFrame [Column "AVG" IntegerType] [[IntegerValue (round avg)]]
        Nothing -> Left $ "Column " ++ columnName ++ " not found in table " ++ tableName
    Nothing -> Left $ "Table " ++ tableName ++ " not found"

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

-- Util functions

getColumnByName :: String -> [Column] -> Column
getColumnByName name cols = fromMaybe (Column "notfound" BoolType) (find (\(Column colName _) -> colName == name) cols)

getColNameList :: [Column] -> [String]
getColNameList = map (\(Column name _) -> name)

getColumnType :: Column -> ColumnType
getColumnType (Column _ columnType) = columnType

getDataFrameByName :: TableName -> DataFrame
getDataFrameByName name = fromMaybe (DataFrame [] []) (lookup name database)

getDataFrameCols :: DataFrame -> [Column]
getDataFrameCols (DataFrame cols _) = cols

getDataFrameRows :: DataFrame -> [Row]
getDataFrameRows (DataFrame _ rows) = rows

isTableInDatabase :: TableName -> Bool
isTableInDatabase name = case lookup name database of
  Just _ -> True
  Nothing -> False

-- Filter rows based on whether the specified column's value is TRUE or FALSE.
filterRowsByBoolColumn :: TableName -> String -> Bool -> Either ErrorMessage DataFrame
filterRowsByBoolColumn name col bool
  | isTableInDatabase name && col `elem` getColNameList (getDataFrameCols (getDataFrameByName name)) && getColumnType (getColumnByName col (getDataFrameCols (getDataFrameByName name))) == BoolType = Right $ getRowsByBool bool (getDataFrameRows (getDataFrameByName name))
  | otherwise = Left "Dataframe does not exist or does not contain column by specified name or column is not of type bool"
  where
    getRowsByBool :: Bool -> [Row] -> DataFrame
    getRowsByBool boolValue tableRows = DataFrame (getDataFrameCols (getDataFrameByName name)) (filter (\row -> rowCellAtIndexIsBool boolValue row $ elemIndex col (getColNameList (getDataFrameCols (getDataFrameByName name)))) tableRows)

    rowCellAtIndexIsBool :: Bool -> Row -> Maybe Int -> Bool
    rowCellAtIndexIsBool boolVal row index = case index of
      Just ind -> row !! ind == BoolValue boolVal
      Nothing -> False

-- max aggregate function
sqlMax :: TableName -> String -> Either ErrorMessage Value
sqlMax name col
  | col `elem` getColNameList (getDataFrameCols (getDataFrameByName name)) && isRightValue (getColumnByName col (getDataFrameCols (getDataFrameByName name))) = Right (maximum'' (getValues (getDataFrameRows (getDataFrameByName name)) (fromMaybe 0 (elemIndex col (getColNameList (getDataFrameCols (getDataFrameByName name)))))))
  | otherwise = Left "Cannot get max of this value type or table does not exist"
  where
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
    
findColumnIndex :: String -> DataFrame -> Maybe Int
findColumnIndex columnName (DataFrame columns _) =
  elemIndex columnName (map (\(Column name _) -> name) columns)

getColumnValue :: Int -> Row -> Value
getColumnValue columnIndex row = row !! columnIndex

isIntegerValue :: Value -> Bool
isIntegerValue (IntegerValue _) = True
isIntegerValue _ = False

sumIntValues :: [Value] -> Integer
sumIntValues = foldr (\(IntegerValue x) acc -> x + acc) 0
