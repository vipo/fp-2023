{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement(..),
  )
where

import Data.Char (toLower)
import DataFrame (DataFrame (..), Column (..), ColumnType (..), Value (..), Row)
import InMemoryTables (TableName, database)
import Data.List (elemIndex, isPrefixOf, isSuffixOf)


type ErrorMessage = String

data ParsedStatement
  = ShowTable TableName
  | ShowTables
  | AvgColumn TableName String
  deriving (Show, Eq)

--parseStatement :: String -> Either ErrorMessage ParsedStatement
--parseStatement input
--  | last input /= ';' = Left "Unsupported or invalid statement"
--  | otherwise =
--    let cleanedInput = init input
--        wordsInput = words (map toLower cleanedInput)
--    in case wordsInput of
--     ["show", "table", tableName] -> Right $ ShowTable tableName
--      ["show", "tables"] -> Right ShowTables
--      "select" : rest ->
--        case rest of
--          func : "from" : tableName : [] 
--            | "avg(" `isPrefixOf` func && ")" `isSuffixOf` func ->
--                let columnName = drop 4 $ init func
--                in Right $ AvgColumn tableName columnName
--          _ -> Left "Unsupported or invalid statement"
--      _ -> Left "Unsupported or invalid statement"

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
        lowerInput = map toLower cleanedInput
        wordsInput = words lowerInput
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
                in if tableNameExists tableName
                   then Right $ AvgColumn tableName columnName
                   else Left "Unsupported or invalid statement"
          _ -> Left "Unsupported or invalid statement"
      _ -> Left "Unsupported or invalid statement"



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
          in
            if null validIntValues
              then Left "No valid integers found in the specified column"
              else
                let sumValues = sumIntValues validIntValues
                    avg = fromIntegral sumValues / fromIntegral (length validIntValues)
                in
                  Right $ DataFrame [Column "AVG" IntegerType] [[IntegerValue (round avg)]]
        Nothing -> Left $ "Column " ++ columnName ++ " not found in table " ++ tableName
    Nothing -> Left $ "Table " ++ tableName ++ " not found"

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

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

