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
import Data.List (elemIndex, find)
import Data.Maybe (fromMaybe)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables (TableName, database)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

data ParsedStatement
  = ShowTableStmt TableName
  | ShowAllTablesStmt
  deriving (Show, Eq)

keywordMatch :: String -> String -> Bool
keywordMatch xs ys = map toLower xs == map toLower ys

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | last input /= ';' = Left "Unsupported or invalid statement"
  | keywordMatch "SHOW TABLES" (init input) = Right ShowAllTablesStmt
  | keywordMatch "SHOW TABLE" (unwords $ take 2 (words input)) && length (words input) > 2 =
      Right $ ShowTableStmt (filter (/= ';') ((words input) !! 2))
  | otherwise = Left "Unsupported or invalid statement"

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTableStmt tableName) =
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowAllTablesStmt =
  Right $
    DataFrame
      [Column "Tables" StringType]
      (map (\(name, _) -> [StringValue name]) database)

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