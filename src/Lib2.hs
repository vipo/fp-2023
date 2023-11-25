{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE InstanceSigs #-}

module Lib2
  ( 
    ColumnName,
    Aggregate,
    AggregateFunction (..),
    And (..),
    SpecialSelect (..),
    AggregateList,
    Operand (..),
    Operator (..),
    Condition (..),
    WhereSelect,
    validateDataFrame,
    parseStatement,
    Parser,
    getColumnName,
    aggregateParser,
    runParser,
    stopParseAt,
    whitespaceParser,
    queryStatementParser,
    operatorParser,
    constantParser,
    Condition,
    trashParser,
    seperate,
    columnNameParser,
    tableNameParser,
    getType,
    char,
    optional,
    many,
    empty,
    some,
    (<|>),
    empty,
    fail,
    isFaultyConditions,
    isFaultyCondition,
    doColumnsExist,
    areRowsEmpty,
    whereConditionColumnList,
    whereConditionColumnName,
    filterSelect,
    filterCondition,
    conditionResult,
    getFilteredValue,
    processSelect,
    processSelectAggregates,
    isIntegerType,
    rowListToRow,
    getColumnNames,
    switchListToTuple,
    getCols,
    getCol,
    getRows,
    getRow,
    findMax,
    findMaxValues,
    findMaxValue,
    findSum,
    findSumValues,
    findSumValue,
    executeStatement,
    isValidTableName,
    isOneWord,
    dropWhiteSpaces,
    columnsToList,
    createColumnsDataFrame,
    createTablesDataFrame
  )
where

import DataFrame
    ( DataFrame(..),
      Row,
      Column(..),
      ColumnType(..),
      Value(..),
      DataFrame)
import InMemoryTables (TableName, database)
import Data.List.NonEmpty (some1, xor)
import Foreign.C (charIsRepresentable)
import Data.Char (toLower, GeneralCategory (ParagraphSeparator), isSpace, isAlphaNum, isDigit, digitToInt)
import qualified InMemoryTables as DataFrame
import Lib1 (renderDataFrameAsTable, findTableByName, parseSelectAllStatement, checkTupleMatch, zipColumnsAndValues, checkRowSizes)
import Data.List (isPrefixOf, nub)
import Data.Maybe (fromMaybe)
import Data.Either
import Text.ParserCombinators.ReadP (get)
import Data.Foldable (find)
import Control.Alternative.Free (Alt(alternatives))

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String

type Aggregate = (AggregateFunction, ColumnName)

data AggregateFunction = Sum | Max
  deriving (Show, Eq)

data And = And
  deriving (Show, Eq)

data SpecialSelect = SelectAggregate AggregateList | SelectColumns [ColumnName] | SelectColumnsTables [(TableName, ColumnName)]
  deriving (Show, Eq)

type AggregateList = [(AggregateFunction, ColumnName)]

data Operand = ColumnOperand ColumnName | ConstantOperand Value | ColumnTableOperand (TableName, ColumnName)
  deriving (Show, Eq)

data Operator =
     IsEqualTo
    |IsNotEqual
    |IsLessThan
    |IsGreaterThan
    |IsLessOrEqual
    |IsGreaterOrEqual
    deriving (Show, Eq)

data Condition = Condition Operand Operator Operand
  deriving (Show, Eq)

type WhereSelect = [Condition]

-- Keep the type, modify constructors
data ParsedStatement =
  ShowTable {
    table :: TableName
   }
  |SelectAll {
    table :: TableName,
    selectWhere :: Maybe WhereSelect
   }
  |Select {
    selectQuery :: SpecialSelect,
    table :: TableName,
    selectWhere :: Maybe WhereSelect
  }
  | ShowTables { }
    deriving (Show, Eq)

data Trash = Trash String
  deriving (Show, Eq)

-----------------------------------------------------------------------------------------------------------
newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (a, String)
}

instance Functor Parser where
  fmap f (Parser x) = Parser $ \s -> do
    (x', s') <- x s
    return (f x', s')

instance Applicative Parser where
  pure :: a -> Parser a
  pure x = Parser $ \s -> Right (x, s)
  (<*>) :: Parser (a -> b) -> Parser a -> Parser b
  (Parser f) <*> (Parser x) = Parser $ \s -> do
    (f', s1) <- f s
    (x', s2) <- x s1
    return (f' x', s2)

instance Monad Parser where
  (>>=) :: Parser a -> (a -> Parser b) -> Parser b
  (Parser x) >>= f = Parser $ \s -> do
    (x', s') <- x s
    runParser (f x') s'

instance MonadFail Parser where
  fail :: String -> Parser a
  fail _ = Parser $ \_ -> Left "Monad failed"

class (Applicative f) => Alternative f where
  empty :: f a
  ( <|> ) :: f a -> f a -> f a
  some :: f a -> f [a]
  some v = some_v
    where many_v = some_v <|> pure []
          some_v = (:) <$> v <*> many_v

  many :: f a -> f [a]
  many v = many_v
    where many_v = some_v <|> pure []
          some_v = (:) <$> v <*> many_v

instance Alternative Parser where
  empty :: Parser a
  empty = fail "empty"
  (<|>) :: Parser a -> Parser a -> Parser a
  (Parser x) <|> (Parser y) = Parser $ \s ->
    case x s of
      Right x -> Right x
      Left _ -> y s

char :: Char -> Parser Char
char c = Parser charP
  where charP []                 = Left "Empty input"
        charP (x:xs) | x == c    = Right (c, xs)
                     | otherwise = Left ("Expected " ++ [c])

optional :: Parser a -> Parser (Maybe a)
optional p = do
  Just <$> p
  <|> return Nothing

instance Ord Value where
    compare :: Value -> Value -> Ordering
    compare (IntegerValue a) (IntegerValue b) = compare a b
    compare (StringValue a) (StringValue b) = compare a b
    compare (BoolValue a) (BoolValue b) = compare a b
    compare NullValue NullValue = EQ
    compare NullValue _ = LT
    compare _ NullValue = GT

-----------------------------------------------------------------------------------------------------------

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement query = case runParser p query of
    Left err1 -> Left err1
    Right (query, rest) -> case query of
        Select _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        ShowTable _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        ShowTables -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query  
        SelectAll _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query  
    where
        p :: Parser ParsedStatement
        p = showTableParser
               <|> showTablesParser
               <|> selectStatementParser
               <|> selectAllParser

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ createTablesDataFrame findTableNames
executeStatement (ShowTable table) = Right (createColumnsDataFrame (columnsToList (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))) table)
executeStatement (Select selectQuery table selectWhere) =
  case doTableExist table of
    True -> case validateDataFrame (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) of
      True -> case selectWhere of
        Just conditions -> case doColumnsExist (whereConditionColumnList conditions) (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) of
          True -> case isFaultyConditions conditions of
            False -> case selectQuery of
              SelectColumns cols -> do
                case doColumnsExist cols (filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions) of 
                  True -> case areRowsEmpty (filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions) of 
                    False -> Right  (uncurry createSelectDataFrame (getColumnsRows cols (filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions)))
                    True -> Left "There are no results with the provided conditions or the condition is faulty"
                  False -> Left "Provided column name does not exist in database or you are mixing aggregate functions and columns"
              SelectAggregate aggList -> do
                  case areRowsEmpty (filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions) of 
                    False -> case processSelect (( filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions)) aggList of
                      Left err -> Left err 
                      Right (newCols, newRows) -> Right $ createSelectDataFrame newCols newRows
                    True -> Left "There are no results with the provided conditions or the condition is faulty"
            True -> Left "Conditions are faulty"
          False -> Left "The specified column doesn't exist" 
        Nothing -> case selectQuery of 
          SelectColumns cols -> do
            (if doColumnsExist cols (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) then Right (uncurry createSelectDataFrame (getColumnsRows cols (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)))
                          ) else Left "Provided column name does not exist in database or you are mixing aggregate functions and columns")
          SelectAggregate aggList -> do
            case processSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) aggList of
              Left err -> Left err
              Right (newCols, newRows) -> Right $ createSelectDataFrame newCols newRows
      False -> Left "The table is not valid"
    False -> Left "Table not found in the database"


executeStatement (SelectAll table selectWhere) =
  case doTableExist table of 
    True -> case validateDataFrame (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) of 
      True -> case selectWhere of
        Just conditions -> case isFaultyConditions conditions of
          False -> case doColumnsExist (whereConditionColumnList conditions) (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) of
            True -> case areRowsEmpty (filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions) of
              True -> Left "There are no results with the provided conditions or the condition is faulty"
              False -> Right (uncurry createSelectDataFrame (getColumnsRows (columnsToList (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))) (filterSelect (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) conditions)))
            False -> Left "The specified column doesn't exist"
          True -> Left "Conditions are faulty"
        Nothing -> Right (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))
      False -> Left "The table is not valid"
    False -> Left "Table not found in the database"  
  
-----------------------------------------------------------------------------------------------------------
isFaultyConditions :: [Condition] -> Bool
isFaultyConditions [] = False
isFaultyConditions (x:xs) 
  | isFaultyCondition x == False = isFaultyConditions xs
  | otherwise = True


isFaultyCondition :: Condition -> Bool
isFaultyCondition (Condition op1 operator op2) = 
  case operator == IsEqualTo || operator == IsNotEqual of
    True -> False
    False -> case op1 of
      ConstantOperand (IntegerValue i1) -> case op2 of
        ConstantOperand (IntegerValue i2) -> False
        _ -> True
      _ -> True

doTableExist :: TableName -> Bool
doTableExist table = do
  case lookup table InMemoryTables.database of
    Just _ -> True
    Nothing -> False

validateDataFrame :: DataFrame -> Bool
validateDataFrame dataFrame
  | not (checkRowSizes dataFrame) = False
  | not (checkTupleMatch (zipColumnsAndValues dataFrame)) = False
  | otherwise = True


areRowsEmpty :: DataFrame -> Bool
areRowsEmpty (DataFrame _ rows) 
  | rows == [] = True
  | otherwise = False

whereConditionColumnList :: [Condition] -> [ColumnName]
whereConditionColumnList [] = []
whereConditionColumnList (x:xs) = whereConditionColumnName x ++ whereConditionColumnList xs

whereConditionColumnName :: Condition -> [ColumnName]
whereConditionColumnName (Condition op1 _ op2) =
  case op1 of
    ColumnOperand name1 -> case op2 of
      ColumnOperand name2 -> [name1] ++ [name2]
      _ -> [name1]
    ConstantOperand _ -> case op2 of
      ColumnOperand name -> [name]
      ConstantOperand _ -> []

filterSelect :: DataFrame -> [Condition] -> DataFrame
filterSelect df [] = df
filterSelect (DataFrame colsOg rowsOg) (x:xs) = filterSelect (DataFrame colsOg $ filterCondition colsOg rowsOg x) xs

filterCondition :: [Column] -> [Row] -> Condition -> [Row]
filterCondition _ [] _ = []
filterCondition columns (x:xs) condition = 
  if conditionResult columns x condition
    then [x] ++ filterCondition columns xs condition
    else filterCondition columns xs condition

conditionResult :: [Column] -> Row -> Condition -> Bool
conditionResult cols row (Condition op1 operator op2) =
  let v1 = getFilteredValue op1 cols row
      v2 = getFilteredValue op2 cols row
  in
    case operator of
    IsEqualTo -> v1 == v2
    IsNotEqual -> v1 /= v2
    IsLessThan -> v1 < v2
    IsGreaterThan -> v1 > v2
    IsLessOrEqual -> v1 <= v2
    IsGreaterOrEqual -> v1 >= v2

getFilteredValue :: Operand -> [Column] -> Row -> Value
getFilteredValue (ConstantOperand value) _ _ = value
getFilteredValue (ColumnOperand columnName) columns row = getValueFromRow row (findColumnIndex columnName columns) 0

processSelect :: DataFrame -> AggregateList -> Either ErrorMessage ([Column],[Row])
processSelect df aggList =
  if doColumnsExist (getColumnNames aggList) df then (case processSelectAggregates df aggList of
    Left err -> Left err
    Right tuple -> Right $ switchListToTuple tuple) else Left "Some of the provided columns do not exist"

processSelectAggregates :: DataFrame -> [(AggregateFunction, ColumnName)] -> Either ErrorMessage [(Column, Row)]
processSelectAggregates _ [] = Right []
processSelectAggregates (DataFrame cols rows) ((func, colName):xs) =
  case func of
    Max -> do
      let maxResult = findMax (rowListToRow (snd (getColumnsRows [colName] (DataFrame cols rows))))
      restResult <- processSelectAggregates (DataFrame cols rows) xs
      let newColumn = Column ("Max " ++ colName) (head (getColumnType [colName] cols))
      return $ (newColumn, fromRight [] maxResult) : restResult
    Sum -> do
      (if isIntegerType (head (fst (getColumnsRows [colName] (DataFrame cols rows)))) then (do
        let maxResult = findSum (rowListToRow (snd (getColumnsRows [colName] (DataFrame cols rows))))
        restResult <- processSelectAggregates (DataFrame cols rows) xs
        let newColumn = Column ("Sum " ++ colName) (head (getColumnType [colName] cols))
        return $ (newColumn, fromRight [] maxResult) : restResult) else Left "Selected column should have integers")

isIntegerType :: Column -> Bool
isIntegerType (Column _ colType)
  | colType == IntegerType = True
  | otherwise = False

rowListToRow :: [Row] -> Row
rowListToRow xs = concat xs

getColumnNames :: [(AggregateFunction, ColumnName)] -> [ColumnName]
getColumnNames aggregates = nub [col | (_, col) <- aggregates]

switchListToTuple :: [(Column, Row)] -> ([Column], [Row])
switchListToTuple [] = ([], [])
switchListToTuple tuple = (getCols tuple, [getRows tuple])

getCols :: [(Column, Row)] -> [Column]
getCols [] = []
getCols ((col, row) : xs) = getCol (col, row) ++ getCols xs

getCol :: (Column, Row) -> [Column]
getCol (col, _) = [col]

getRows :: [(Column, Row)] -> [Value]
getRows [] = []
getRows ((col, row) : xs) = getRow (col, row) ++ getRows xs

getRow :: (Column, [Value]) -> [Value]
getRow (_, val) = val

findMax :: [Value] -> Either ErrorMessage [Value]
findMax values =
  case filter (/= NullValue) values of
    [] -> Left "Column has no values."
    vals -> Right [findMaxValues vals]

findMaxValues :: [Value] -> Value
findMaxValues = foldl1 findMaxValue

findMaxValue :: Value -> Value -> Value
findMaxValue (IntegerValue a) (IntegerValue b) = IntegerValue (max a b)
findMaxValue (StringValue a) (StringValue b) = StringValue (max a b)
findMaxValue (BoolValue a) (BoolValue b) = BoolValue (a || b)
findMaxValue _ _ = NullValue

findSum :: [Value] -> Either ErrorMessage [Value]
findSum values =
  case filter (/= NullValue) values of
    [] -> Left "Column has no values."
    vals -> Right [findSumValues vals]

findSumValues :: [Value] -> Value
findSumValues = foldl1 findSumValue

findSumValue :: Value -> Value -> Value
findSumValue (IntegerValue a) (IntegerValue b) = IntegerValue (a + b)
findSumValue (IntegerValue a) NullValue = IntegerValue a
findSumValue NullValue (IntegerValue b) = IntegerValue b
findSumValue _ _ = NullValue

-----------------------------------------------------------------------------------------------------------

queryStatementParser :: String -> Parser String
queryStatementParser queryStatement = Parser $ \query ->
    case take (length queryStatement) query of
        [] -> Left "Expected ;"
        xs
            | map toLower xs == map toLower queryStatement -> Right (xs, drop (length xs) query)
            | otherwise -> Left $ "Expected " ++ queryStatement ++ " or query contains unnecessary words"

whitespaceParser :: Parser String
whitespaceParser = Parser $ \query ->
    case span isSpace query of
        ("", _) -> Left $ "Expected whitespace before " ++ query
        (rest, whitespace) -> Right (rest, whitespace)

-----------------------------------------------------------------------------------------------------------

showTablesParser :: Parser ParsedStatement
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser ParsedStatement
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- tableNameParser
    _ <- optional whitespaceParser
    pure $ ShowTable table

-----------------------------------------------------------------------------------------------------


tableNameParser :: Parser TableName
tableNameParser = Parser $ \query ->
  (if isValidTableName query then (case lookup (dropWhiteSpaces (init query)) InMemoryTables.database of
  Just _ -> Right (init (dropWhiteSpaces query), ";")
  Nothing -> Left "Table not found in the database or not provided") else Left "Query does not end with ; or contains unnecessary words after table name")

isValidTableName :: String -> Bool
isValidTableName str
  | dropWhiteSpaces str == "" = False
  | last str == ';' = isOneWord (init str)
  | otherwise = False

isOneWord :: String -> Bool
isOneWord [] = True
isOneWord (x:xs)
  | x /= ' ' = isOneWord xs
  | x == ' ' = dropWhiteSpaces xs == ";"

dropWhiteSpaces :: String -> String
dropWhiteSpaces [] = []
dropWhiteSpaces (x:xs)
  | x /= ' ' = x : dropWhiteSpaces xs
  | otherwise = dropWhiteSpaces xs

columnsToList :: DataFrame -> [ColumnName]
columnsToList (DataFrame [] []) = []
columnsToList (DataFrame columns _) = map getColumnName columns

getColumnName :: Column -> ColumnName
getColumnName (Column "" _) = ""
getColumnName (Column columnname _) = columnname

findTableNames :: [ColumnName]
findTableNames = findTuples InMemoryTables.database

findTuples :: Database -> [ColumnName]
findTuples [] = []
findTuples db = map firstFromTuple db

firstFromTuple :: (ColumnName, DataFrame) -> ColumnName
firstFromTuple = fst

-----------------------------------------------------------------------------------------------------------

selectStatementParser :: Parser ParsedStatement
selectStatementParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    specialSelect <- selectDataParser
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    table <- columnNameParser
    selectWhere <- optional whereParser
    _ <- optional whitespaceParser

    pure $ Select specialSelect table selectWhere
-----------------------------------------------------------------------------------------------------------

trashParser :: Parser Trash 
trashParser = Parser $ \query -> 
  case head query == ',' of 
    True -> Left "Columns are not listed right"
    False -> case "from" `isPrefixOf` (dropWhiteSpaces query) of 
      True -> Right (Trash "",query)
      False -> Left "Columns are not listed right or aggregate functions and column names cannot be mixed"

selectDataParser :: Parser SpecialSelect
selectDataParser = tryParseAggregate <|> tryParseColumn
  where
    tryParseAggregate = do
      aggregateList <- seperate aggregateParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectAggregate aggregateList
    tryParseColumn = do
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectColumns columnNames

aggregateParser :: Parser Aggregate
aggregateParser = do
    func <- aggregateFunctionParser
    _ <- optional whitespaceParser
    _ <- char '('
    _ <- optional whitespaceParser
    columnName <- columnNameParser'
    _ <- optional whitespaceParser
    _ <- char ')'
    pure (func, columnName)

aggregateFunctionParser :: Parser AggregateFunction
aggregateFunctionParser = sumParser <|> maxParser
  where
    sumParser = do
        _ <- queryStatementParser "sum"
        pure Sum
    maxParser = do
        _ <- queryStatementParser "max"
        pure Max

columnNameParser :: Parser ColumnName
columnNameParser = Parser $ \query ->
  case takeWhile (\x -> isAlphaNum x || x == '_') query of
    [] -> Left "Empty input"
    xs -> Right (xs, drop (length xs) query)

seperate :: Parser a -> Parser b -> Parser [a]
seperate p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

whereParser :: Parser WhereSelect
whereParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "where"
  _ <- whitespaceParser
  some whereAndExist
  
  where 
    whereAndExist :: Parser Condition
    whereAndExist = do 
      condition <- whereConditionParser
      _ <- optional (whitespaceParser >> andParser)
      pure condition

andParser :: Parser And
andParser = queryStatementParser "and" >> pure And

whereConditionParser :: Parser Condition
whereConditionParser = do
  _ <- optional whitespaceParser
  operand1 <- operandParser
  _ <- optional whitespaceParser 
  operator <- operatorParser
  _ <- optional whitespaceParser
  operand2 <- operandParser
  return $ Condition operand1 operator operand2

operandParser :: Parser Operand
operandParser = (ConstantOperand <$> constantParser)
               <|> (ColumnOperand <$> columnNameParser)

constantParser :: Parser Value
constantParser = Parser $ \query ->
  case query == "" of 
    True -> Left "The query does not end with a ;"
    False -> 
      let operand = getOperand query
          restQuery = drop (length operand) query
      in case head query == ';' of
          False -> case head operand == '\'' && last operand == '\'' of
            True -> Right (StringValue (init (tail operand)), restQuery)
            False -> case operand of
              "True" -> Right (BoolValue True, restQuery)
              "False" -> Right (BoolValue False, restQuery)
              "null" -> Right (NullValue, restQuery)
              _ ->  if isNumber operand
                    then Right (IntegerValue $ stringToInt operand, restQuery)
                    else Left "Operand is not valid"
          True -> Left "The conditions are missing"

stringToInt :: String -> Integer
stringToInt = foldl (\acc x -> acc * 10 + toInteger (fromEnum x - fromEnum '0')) 0

operatorParser :: Parser Operator
operatorParser = 
  (queryStatementParser "=" >> pure IsEqualTo)
  <|> (queryStatementParser "!=" >> pure IsNotEqual)
  <|> (queryStatementParser "<" >> pure IsLessThan)
  <|> (queryStatementParser ">" >> pure IsGreaterThan)
  <|> (queryStatementParser "<=" >> pure IsLessOrEqual)
  <|> (queryStatementParser ">=" >> pure IsGreaterOrEqual)

-----------------------------------------------------------------------------------------------------

selectAllParser :: Parser ParsedStatement
selectAllParser = do
  _ <- queryStatementParser "select"
  _ <- whitespaceParser
  _ <- queryStatementParser "*"
  _ <- whitespaceParser
  _ <- queryStatementParser "from"
  _ <- whitespaceParser
  table <- columnNameParser
  selectWhere <- optional whereParser
  _ <- optional whitespaceParser
  pure $ SelectAll table selectWhere

stringToBool :: String -> Bool
stringToBool "True" = True
stringToBool "False" = False

isNumber :: String -> Bool
isNumber [] = True
isNumber (x:xs)
  | isDigit x = isNumber xs
  | otherwise = False

getOperand :: String -> String
getOperand [] = []
getOperand (x:xs)
  | x == '=' || x == '>' || x == '<' || x == ' '|| x == ';'|| x == '!' = ""
  | otherwise = x : getOperand xs

isBool :: String -> Bool
isBool str
  | str == "True" || str == "False" = True
  | otherwise = False

-----------------------------------------------------------------------------------------------------------

columnNameParser' :: Parser ColumnName
columnNameParser' = Parser $ \query ->
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses query)) then Right (dropWhiteSpaces (fst (splitStatementAtParentheses query)), snd (splitStatementAtParentheses query)) else Left "There is more than one column name in aggregation function")

areSpacesBetweenWords :: String -> Bool
areSpacesBetweenWords [] = True
areSpacesBetweenWords (x:xs)
  | x == ' ' = dropWhiteSpacesUntilName xs == ""
  | otherwise = areSpacesBetweenWords xs

splitStatementAtParentheses :: String -> (String, String)
splitStatementAtParentheses = go [] where
  go _ [] = ("", "")
  go prefix str@(x:xs)
    | ")" `isPrefixOf` toLowerString str = (reverse prefix, str)
    | otherwise = go (x:prefix) xs

areColumnsListedRight :: String -> Bool
areColumnsListedRight str
  | str == "" = False
  | last (dropWhiteSpaces str) == ','  || head (dropWhiteSpaces str) == ',' =  False
  | otherwise = True

doColumnsExist :: [ColumnName] -> DataFrame -> Bool
doColumnsExist [] _ = True
doColumnsExist (x:xs) df =
    let dfColumnNames = columnsToList df
    in
      ((x `elem` dfColumnNames) && doColumnsExist xs df)

splitStatementAtFrom :: String -> (String, String)
splitStatementAtFrom = go [] where
  go _ [] = ("", "")
  go prefix str@(x:xs)
    | " from" `isPrefixOf` toLowerString str = (reverse prefix, str)
    | otherwise = go (x:prefix) xs

split :: String -> Char -> [String]
split [] _ = [""]
split (c:cs) delim
    | c == delim = "" : rest
    | otherwise = (c : head rest) : tail rest
    where
        rest = split cs delim

commaBetweenColumsNames :: String -> Bool
commaBetweenColumsNames [] = True
commaBetweenColumsNames (x:xs)
  | x /= ',' && xs == "" = True
commaBetweenColumsNames (x:y:xs)
  | x == ' ' && y /=  ' ' && xs == "" = False
  | x /= ',' && y == ' ' && xs == "" = True
  | x /= ',' && xs == "" = True
  | x == ' ' && xs == "" = True
  | x == ',' && y == ' ' && xs == "" = False
  | x == ',' && y /= ' ' && xs == "" = True
  | x /= ' ' && x /= ',' = commaBetweenColumsNames (y:xs)
  | x == ',' && y /= ' ' && y /= ',' = commaBetweenColumsNames (y:xs)
  | x == ',' && whitespaceBeforeNameAfterCommaExist (y:xs) = commaBetweenColumsNames (dropWhiteSpacesUntilName (y:xs))
  | x == ' ' && commaAfterWhitespaceExist (y:xs) = commaBetweenColumsNames (y:xs)
  |otherwise = False

dropWhiteSpacesUntilName :: String -> String
dropWhiteSpacesUntilName [] = []
dropWhiteSpacesUntilName (x:xs)
  | x == ' ' = dropWhiteSpacesUntilName xs
  | otherwise = xs

whitespaceBeforeNameAfterCommaExist :: String -> Bool
whitespaceBeforeNameAfterCommaExist [] = False
whitespaceBeforeNameAfterCommaExist (x:y:xs)
  | x == ' ' && xs == "" = True
  | x /= ' ' && xs == "" = False
  | x == ' ' && y /= ' ' && y /= ',' = True
  | x == ' ' = whitespaceBeforeNameAfterCommaExist (y:xs)
  | otherwise = False

commaAfterWhitespaceExist :: String -> Bool
commaAfterWhitespaceExist [] = True
commaAfterWhitespaceExist (x:xs)
  | x == ' ' = commaAfterWhitespaceExist xs
  | x == ',' = True
  | otherwise = False

getColumnsRows :: [ColumnName] -> DataFrame -> ([Column], [Row])
getColumnsRows colList (DataFrame col row) = (getColumnList colList (getColumnType colList col) , getNewRows col row colList)

getNewRows :: [Column] -> [Row] -> [ColumnName] -> [Row]
getNewRows _ [] _ = []
getNewRows cols (x:xs) colNames = getNewRow x cols colNames : getNewRows cols xs colNames

getNewRow :: [Value] -> [Column] -> [ColumnName] -> [Value]
getNewRow _ _ [] = []
getNewRow row cols (x:xs) = getValueFromRow row (findColumnIndex x cols) 0 : getNewRow row cols xs

getValueFromRow :: Row -> Int -> Int -> Value
getValueFromRow (x:xs) index i
  | index == i = x
  | otherwise = getValueFromRow xs index (i+1)

-----------------------------------------------------------------------------------------------------------

getColumnType :: [ColumnName] -> [Column] -> [ColumnType]
getColumnType [] _ = []
getColumnType (x:xs) col = columnType col 0 (findColumnIndex x col) : getColumnType xs col

columnType :: [Column] -> Int -> Int -> ColumnType
columnType (x:xs) i colIndex
  | i == colIndex = getType x
  | otherwise = columnType xs (i+1) colIndex

getType :: Column -> ColumnType
getType (Column _ colType) = colType

getColumnList :: [ColumnName] -> [ColumnType] -> [Column]
getColumnList [] [] = []
getColumnList (x:xs) (y:ys) = Column x y : getColumnList xs ys

findColumnIndex :: ColumnName -> [Column] -> Int
findColumnIndex columnName columns = columnIndex columnName columns 0

columnIndex :: ColumnName -> [Column] -> Int -> Int
columnIndex _ [] _ = -1
columnIndex columnName ((Column name _):xs) index
    | columnName /= name = (columnIndex columnName xs (index + 1))
    | otherwise = index

toLowerString :: String -> String
toLowerString xs = concatMap (charToString . toLower) xs

charToString :: Char -> String
charToString c = [c]
 
-----------------------------------------------------------------------------------------------------------

createColumnsDataFrame :: [ColumnName] -> TableName -> DataFrame
createColumnsDataFrame columnNames columnTableName = DataFrame [Column columnTableName StringType] (map (\name ->  [StringValue name]) columnNames)

createSelectDataFrame :: [Column] -> [Row] -> DataFrame
createSelectDataFrame columns rows = DataFrame columns rows

createTablesDataFrame :: [TableName] -> DataFrame
createTablesDataFrame tableNames = DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

-----------------------------------------------------------------------------------------------------------

stopParseAt :: Parser String
stopParseAt  = do
  _ <- optional whitespaceParser
  _ <- queryStatementParser ";"
  checkAfterQuery
  where
    checkAfterQuery :: Parser String
    checkAfterQuery = Parser $ \query ->
        case query of
            [] -> Right ([], [])
            s -> Left ("Characters found after ;" ++ s)
