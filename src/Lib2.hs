{-# OPTIONS_GHC -Wno-dodgy-imports #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    Operator (..),
    ParsedStatement (..),
  )
where

import Data.Char (isDigit, isSpace, toLower)
import Data.List (elemIndex, find, isPrefixOf)
import Data.Maybe (fromJust)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row (..), Value (..))
import InMemoryTables (TableName, database)
import Lib1 ()
import Text.Read (readMaybe)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

data Operator = Operator String String Value deriving (Show, Eq)

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTables
  | ShowTable TableName
  | Select [String] TableName (Maybe [Operator])
  | ParsedStatement
  | Where [Operator]
  deriving (Show, Eq)

instance Ord Value where
  compare (StringValue s1) (StringValue s2) = compare s1 s2
  compare _ _ = EQ -- Handle other Value constructors, e.g., handle Null or other types

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | null input = Left "Empty input"
  | last input == ';' = parseStatement (init input)
  | otherwise =
      let parseableList = replaceKeywordsToLower (splitStringIntoWords input)
       in case parseableList of
            ["show", "tables"] -> Right ShowTables
            ["show", "table", table] -> Right (ShowTable table)
            ("select" : columns) ->
              case break (== "from") columns of
                (cols, "from" : tableName : "where" : rest) -> do
                  (conditions, _) <- parseWhereConditions rest
                  if null conditions
                    then Left "Invalid WHERE statement"
                    else Right (Select cols tableName (Just conditions))
                (cols, "from" : tableName : _) -> Right (Select cols tableName Nothing)
                _ -> Left "Invalid SELECT statement"
            _ -> Left "Not supported statement"

replaceKeywordsToLower :: [String] -> [String]
replaceKeywordsToLower replaceInput = map replaceKeyword replaceInput
  where
    replaceKeyword :: String -> String
    replaceKeyword keyword
      | keyword == getKeywordCaseSensitive "show" replaceInput = "show"
      | keyword == getKeywordCaseSensitive "table" replaceInput = "table"
      | keyword == getKeywordCaseSensitive "tables" replaceInput = "tables"
      | keyword == getKeywordCaseSensitive "select" replaceInput = "select"
      | keyword == getKeywordCaseSensitive "from" replaceInput = "from"
      | keyword == getKeywordCaseSensitive "where" replaceInput = "where"
      | keyword == getKeywordCaseSensitive "and" replaceInput = "and"
      | keyword == getKeywordCaseSensitive "or" replaceInput = "or"
      | keyword == getKeywordCaseSensitive "not" replaceInput = "not"
      | otherwise = keyword

toLowerPrefix :: String -> String -> Bool
toLowerPrefix prefix str = map toLower prefix `isPrefixOf` map toLower str

parseWhereConditions :: [String] -> Either ErrorMessage ([Operator], [String])
-- parseWhereConditions [] = Left "Invalid WHERE statement"
parseWhereConditions ("and" : rest)
  | null rest = Left "Incomplete AND statement"
  | otherwise = do
      (operators, remaining) <- parseWhereConditions rest
      Right (operators, remaining)
parseWhereConditions (colName : op : value : rest)
  | not (null rest) && head rest /= "and" = Left "Invalid WHERE statement"
  | head value == '\"' = do
      let (stringValue, remainingRest) = collectStringValue (value : rest)
      if last stringValue == '\"'
        then case op of
          "=" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName "=" (StringValue (init (tail stringValue))) : operators, remaining)
          "/=" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName "/=" (StringValue (init (tail stringValue))) : operators, remaining)
          "<>" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName "<>" (StringValue (init (tail stringValue))) : operators, remaining)
          "<" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName "<" (StringValue (init (tail stringValue))) : operators, remaining)
          ">" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName ">" (StringValue (init (tail stringValue))) : operators, remaining)
          "<=" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName "<=" (StringValue (init (tail stringValue))) : operators, remaining)
          ">=" -> do
            (operators, remaining) <- parseWhereConditions remainingRest
            Right (Operator colName ">=" (StringValue (init (tail stringValue))) : operators, remaining)
          _ -> Left "Invalid operator"
        else Left "A string should end with \""
  | otherwise =
      case op of
        "=" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName "=" (IntegerValue intValue) : operators, remaining)
          Nothing -> case value of
            "True" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "=" (BoolValue True) : operators, remaining)
            "False" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "=" (BoolValue False) : operators, remaining)
            "NULL" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "=" NullValue : operators, remaining)
            _ -> do
              Left "Strings should be surrounded by double quotes"
        "/=" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName "/=" (IntegerValue intValue) : operators, remaining)
          Nothing -> case value of
            "true" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "/=" (BoolValue True) : operators, remaining)
            "false" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "/=" (BoolValue False) : operators, remaining)
            "null" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "/=" NullValue : operators, remaining)
            _ -> do
              Left "Strings should be surrounded by double quotes"
        "<>" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName "<>" (IntegerValue intValue) : operators, remaining)
          Nothing -> case value of
            "true" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "<>" (BoolValue True) : operators, remaining)
            "false" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "<>" (BoolValue False) : operators, remaining)
            "null" -> do
              (operators, remaining) <- parseWhereConditions rest
              Right (Operator colName "<>" NullValue : operators, remaining)
            _ -> do
              Left "Strings should be surrounded by double quotes"
        "<" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName "<" (IntegerValue intValue) : operators, remaining)
          Nothing -> do
            Left "Strings should be surrounded by double quotes"
        ">" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName ">" (IntegerValue intValue) : operators, remaining)
          Nothing -> do
            Left "Strings should be surrounded by double quotes"
        "<=" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName "<=" (IntegerValue intValue) : operators, remaining)
          Nothing -> do
            Left "Strings should be surrounded by double quotes"
        ">=" -> case readMaybe value of
          Just intValue -> do
            (operators, remaining) <- parseWhereConditions rest
            Right (Operator colName ">=" (IntegerValue intValue) : operators, remaining)
          Nothing -> do
            Left "Strings should be surrounded by double quotes"
        _ -> Left "Invalid operator"
parseWhereConditions _ = Right ([], [])

collectStringValue :: [String] -> (String, [String])
collectStringValue [] = ("", [])
collectStringValue (x : xs)
  | last x == '\"' = (x, xs)
  | otherwise = let (value, remaining) = collectStringValue xs in (x ++ " " ++ value, remaining)

{- parseWhereConditions (colName : op : value : rest) =
  case op of
    "=" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "=" (IntegerValue intValue) : operators, remaining)
      Nothing -> case value of
        "True" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "=" (BoolValue True) : operators, remaining)
        "False" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "=" (BoolValue False) : operators, remaining)
        "NULL" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "=" NullValue : operators, remaining)
        _ -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "=" (StringValue value) : operators, remaining)
    "/=" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "/=" (IntegerValue intValue) : operators, remaining)
      Nothing -> case value of
        "true" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "/=" (BoolValue True) : operators, remaining)
        "false" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "/=" (BoolValue False) : operators, remaining)
        "null" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "/=" NullValue : operators, remaining)
        _ -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "/=" (StringValue value) : operators, remaining)
    "<>" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "<>" (IntegerValue intValue) : operators, remaining)
      Nothing -> case value of
        "true" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "<>" (BoolValue True) : operators, remaining)
        "false" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "<>" (BoolValue False) : operators, remaining)
        "null" -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "<>" NullValue : operators, remaining)
        _ -> do
          (operators, remaining) <- parseWhereConditions rest
          Right (Operator colName "<>" (StringValue value) : operators, remaining)
    "<" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "<" (IntegerValue intValue) : operators, remaining)
      Nothing -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "<" (StringValue value) : operators, remaining)
    ">" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName ">" (IntegerValue intValue) : operators, remaining)
      Nothing -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName ">" (StringValue value) : operators, remaining)
    "<=" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "<=" (IntegerValue intValue) : operators, remaining)
      Nothing -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName "<=" (StringValue value) : operators, remaining)
    ">=" -> case readMaybe value of
      Just intValue -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName ">=" (IntegerValue intValue) : operators, remaining)
      Nothing -> do
        (operators, remaining) <- parseWhereConditions rest
        Right (Operator colName ">=" (StringValue value) : operators, remaining)
    _ -> Left "Invalid operator"
-- parseWhereConditions (_ : _) = Left "Invalid WHERE statement"
parseWhereConditions _ = Right ([], []) -}

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- ExecuteStatement function
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right showTables
executeStatement (ShowTable tablename) =
  case lookup tablename database of
    Just df -> Right $ DataFrame [Column "columns" StringType] (map (\col -> [StringValue (getColumnName col)]) (getColumns df))
    Nothing -> Left "Table not found"
executeStatement (Select columnNames tableName maybeOperator)
  | null columnNames = Left "No columns provided"
  | null tableName = Left "No table provided"
  | (("min(" `toLowerPrefix` head columnNames || "sum(" `toLowerPrefix` head columnNames) && null (tail columnNames)) || not ("min(" `toLowerPrefix` head columnNames || "sum(" `toLowerPrefix` head columnNames) = case lookup tableName database of
      Just df -> do
        let pureColumnNames = map extractColumnNameFromFunction columnNames
        let missingColumns = filter (\colName -> not (any (\col -> getColumnName col == colName) (getColumns df))) pureColumnNames
        if not (null missingColumns) && head pureColumnNames /= "*" -- Check if all columns inputed exist
          then Left ("Column not found: " ++ unwords missingColumns)
          else Right ()
        let filteredDataFrame = case maybeOperator of
              Just operators -> filterDataFrameByOperators operators df -- WHERE clause
              Nothing -> df -- No WHERE clause
        let filteredRows = getRows filteredDataFrame
        let colIndex =
              if "min(" `toLowerPrefix` head columnNames || "sum(" `toLowerPrefix` head columnNames
                then columnIndex df (Column (head columnNames) StringType)
                else -1
        let selectedCols
              | "min(" `toLowerPrefix` head columnNames = [Column "minimum" (columnType (getColumns df !! colIndex))]
              | "sum(" `toLowerPrefix` head columnNames = [Column "sum" (columnType (getColumns df !! colIndex))]
              | "*" `elem` columnNames = getColumns df
              | otherwise = filter (\col -> getColumnName col `elem` columnNames) (getColumns df)
        let selectedIndices = map (columnIndex df) selectedCols
        let minValResult = case filteredRows of
              [] -> NullValue
              _ -> foldl1 (minValue colIndex (columnType (getColumns df !! colIndex))) (map (!! colIndex) filteredRows)
        let selectedRows
              | getColumnName (head selectedCols) == "minimum" = [[minValResult]]
              | getColumnName (head selectedCols) == "sum" = [[calculateSum colIndex filteredRows]]
              | otherwise = map (\row -> map (row !!) selectedIndices) filteredRows
        Right $ DataFrame selectedCols selectedRows
      Nothing -> Left "Table not found"
  | otherwise = Left "Cannot use agregate functions with multiple columns"
executeStatement _ = Left "Not implemented"

extractColumnNameFromFunction :: String -> String
extractColumnNameFromFunction columnName
  | "min(" `toLowerPrefix` columnName = drop 3 (init (tail columnName))
  | "sum(" `toLowerPrefix` columnName = drop 3 (init (tail columnName))
  | otherwise = columnName

-- Helper functions
showTables :: DataFrame
showTables = DataFrame [Column "tables" StringType] (map (return . StringValue . fst) database)

maybeTableToEither :: Maybe DataFrame -> Either ErrorMessage DataFrame
maybeTableToEither (Just df) = Right df
maybeTableToEither Nothing = Left "Table not found"

maybeFilterRows :: DataFrame -> Maybe Operator -> Maybe [Row]
maybeFilterRows df maybeOperator = case maybeOperator of
  Just (Operator colName op compCase) -> Just $ filter (evalCondition colName op compCase df) (getRows df)
  Nothing -> Just $ getRows df

filterColumns :: [String] -> DataFrame -> [Column]
filterColumns columnNames df = filter (\col -> getColumnName col `elem` columnNames) (getColumns df)

selectRow :: DataFrame -> [Column] -> Row -> Row
selectRow df selectedCols row = map (\col -> row !! columnIndex df col) selectedCols

evalCondition :: String -> String -> Value -> DataFrame -> Row -> Bool
-- evalCondition "and" colName val df row = any (\cond -> evalCondition cond df row) conditions
evalCondition colName "=" val df row = matchValue (getColumnValue colName df row) val
evalCondition colName "/=" val df row = not (matchValue (getColumnValue colName df row) val)
evalCondition colName "<>" val df row = not (matchValue (getColumnValue colName df row) val)
evalCondition colName "<" val df row = compareValues (<) colName val df row
evalCondition colName ">" val df row = compareValues (>) colName val df row
evalCondition colName "<=" val df row = compareValues (<=) colName val df row
evalCondition colName ">=" val df row = compareValues (>=) colName val df row
evalCondition _ _ _ _ _ = False

columnType :: Column -> ColumnType
columnType (Column _ colType) = colType

filterDataFrameByOperators :: [Operator] -> DataFrame -> DataFrame
filterDataFrameByOperators operators df =
  foldr filterDataFrameByOperator df operators

filterDataFrameByOperator :: Operator -> DataFrame -> DataFrame
filterDataFrameByOperator (Operator colName op compCase) = filterDataFrameByColumnValue colName op compCase

-- Function to filter a DataFrame based on a specified column value
filterDataFrameByColumnValue :: String -> String -> Value -> DataFrame -> DataFrame
filterDataFrameByColumnValue colName op val df = do
  let filteredRows = filter (evalCondition colName op val df) (getRows df)
  DataFrame (getColumns df) filteredRows {- matchValue (getColumnValue colName df row) val) (getRows df) -}

getColumn :: String -> DataFrame -> Column
getColumn colName df = case find (\col -> getColumnName col == colName) (getColumns df) of
  Just col -> col
  Nothing -> error "Column not found"

-- Helper function to find the column index by name
findColumnIndex :: String -> [Column] -> Maybe Int
findColumnIndex colName cols = elemIndex colName (map getColumnName cols)

matchValue :: Value -> Value -> Bool
matchValue (StringValue s1) (StringValue s2) = s1 == s2
matchValue (IntegerValue i1) (IntegerValue i2) = i1 == i2
matchValue (BoolValue b1) (BoolValue b2) = b1 == b2
matchValue NullValue NullValue = True
matchValue _ _ = False

trimValue :: Value -> Value
trimValue (StringValue s) = StringValue (trimWhitespace s)
trimValue v = v

getColumnValue :: String -> DataFrame -> Row -> Value
getColumnValue colName df row =
  let colIndex = columnIndex df (Column (trimWhitespace (map toLower colName)) StringType)
   in row !! colIndex

trimWhitespace :: String -> String
trimWhitespace = filter (not . isSpace)

compareValues :: (String -> String -> Bool) -> String -> Value -> DataFrame -> Row -> Bool
compareValues op colName val df row =
  case (trimValue (getColumnValue colName df row), trimValue val) of
    (StringValue s1, StringValue s2) -> op (map toLower s1) (map toLower s2)
    (IntegerValue i1, IntegerValue i2) -> op (show i1) (show i2)
    _ -> False

getColumnName :: Column -> String
getColumnName (Column name _)
  | toLowerPrefix "min(" name = init $ tail $ dropWhile (/= '(') name
  | toLowerPrefix "sum(" name = init $ tail $ dropWhile (/= '(') name
getColumnName (Column name _) = name

columnIndex :: DataFrame -> Column -> Int
columnIndex (DataFrame cols _) col = case find (\c -> getColumnName c == getColumnName col) cols of
  Just foundCol -> fromJust $ elemIndex foundCol cols
  -- if Nothing then return error "Column not found" and the name of the column that was not found
  Nothing -> error $ "Column not found: " ++ getColumnName col

getRows :: DataFrame -> [Row]
getRows (DataFrame _ rs) = rs

getColumns :: DataFrame -> [Column]
getColumns (DataFrame cols _) = cols

minVal :: String -> DataFrame -> Maybe Value
minVal colName (DataFrame cols rows) = do
  colIndex <- findColumnIndex colName cols
  let colValues = map (!! colIndex) rows
  case colValues of
    [] -> Nothing -- Empty column
    _ -> Just (minimum colValues)

minValue :: Int -> ColumnType -> Value -> Value -> Value
minValue _ IntegerType (IntegerValue i1) (IntegerValue i2) = if i1 < i2 then IntegerValue i1 else IntegerValue i2
minValue _ BoolType (BoolValue b1) (BoolValue b2) = if b1 < b2 then BoolValue b1 else BoolValue b2
minValue _ StringType (StringValue s1) (StringValue s2) = if s1 < s2 then StringValue s1 else StringValue s2
minValue _ _ val1 NullValue = val1
minValue _ _ NullValue val2 = val2
minValue colIndex colType _ _ = error $ "Column type mismatch for column at index " ++ show colIndex ++ ". Expected " ++ show colType

calculateSum :: Int -> [Row] -> Value
calculateSum colIndex rows =
  let totalSum = sum [case row !! colIndex of IntegerValue i -> i; _ -> 0 | row <- rows]
      count = toInteger (length rows)
   in if count > 0 then IntegerValue totalSum else NullValue

splitStringIntoWords :: String -> [String]
splitStringIntoWords = words

getKeywordCaseSensitive :: String -> [String] -> String
getKeywordCaseSensitive keyword (x : xs) = if map toLower x == keyword then x else getKeywordCaseSensitive keyword xs
getKeywordCaseSensitive _ [] = []

splitSQL :: String -> String -> (String, String)
splitSQL keyword input = case breakOnCaseInsensitive keyword input of
  Just (select, rest) -> (select, rest)
  Nothing -> ("", input)

breakOnCaseInsensitive :: String -> String -> Maybe (String, String)
breakOnCaseInsensitive _ [] = Nothing
breakOnCaseInsensitive toFind text@(t : ts) =
  if isPrefixCaseInsensitive toFind text
    then Just (text, "")
    else case breakOnCaseInsensitive toFind ts of
      Just (before, after) -> Just (t : before, after)
      Nothing -> Nothing

isPrefixCaseInsensitive :: String -> String -> Bool
isPrefixCaseInsensitive [] _ = True
isPrefixCaseInsensitive _ [] = False
isPrefixCaseInsensitive (a : as) (b : bs) = toLower a == toLower b && isPrefixCaseInsensitive as bs
