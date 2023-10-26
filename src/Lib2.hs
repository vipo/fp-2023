{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# HLINT ignore "Redundant return" #-}
{-# OPTIONS_GHC -Wno-orphans #-}


module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    queryStatementParser,
    whitespaceParser,
    showTablesParser,
    showTableParser,
    tableNameParser,
    isValidTableName,
    isOneWord,
    dropWhiteSpaces,
    columnsToList,
    getColumnName,
    findTableNames,
    findTuples,
    firstFromTuple,
    selectStatementParser,
    columnNamesParser,
    areColumnsListedRight,
    splitStatementAtFrom,
    split,
    toLowerString,
    charToString,
    createColumnsDataFrame,
    createColumnsDataFrame,
    stopParseAt
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
import Data.Char (toLower, GeneralCategory (ParagraphSeparator), isSpace)
import qualified InMemoryTables as DataFrame
import Lib1 (renderDataFrameAsTable, findTableByName)
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)
import Text.ParserCombinators.ReadP (get)
import Data.Foldable (find)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String

data Aggregate = Aggregate AggregateFunction ColumnName
  deriving (Show, Eq)

data AggregateFunction = Sum | Max
  deriving (Show, Eq)

data SpecialSelect =
  SelectAggregate Aggregate
  | SelectColumns [ColumnName]
  deriving (Show, Eq)

-- Keep the type, modify constructors
data ParsedStatement =
  Select {
    selectQuery :: SpecialSelect, --column :: [ColumnName],
    table :: TableName
  }
  | ShowTable {
    table :: TableName
   }
  | ShowTables { }
    deriving (Show, Eq)

--------------------------------------------------------------------------------
newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (a, String)
}

instance Functor Parser where
  fmap f (Parser x) = Parser $ \s -> do
    (x', s') <- x s
    return (f x', s')

instance Applicative Parser where
  pure x = Parser $ \s -> Right (x, s)
  (Parser f) <*> (Parser x) = Parser $ \s -> do
    (f', s1) <- f s
    (x', s2) <- x s1
    return (f' x', s2)

instance Monad Parser where
  (Parser x) >>= f = Parser $ \s -> do
    (x', s') <- x s
    runParser (f x') s'

instance MonadFail Parser where
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
  empty = fail "empty"
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
  result <- p
  return (Just result)
  <|> return Nothing

----------------------------------------------------------------------------------

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement query = case runParser p query of
    Left err1 -> Left err1
    Right (query, rest) -> case query of
        ShowTables -> case runParser stopParseAt rest of
            Left err2 -> Left err2
            Right _ -> Right query
        ShowTable _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Select _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
    where
        p :: Parser ParsedStatement
        p = showTablesParser
               <|> showTableParser
               <|> selectStatementParser


executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ createTablesDataFrame findTableNames
executeStatement (ShowTable table) = Right (createColumnsDataFrame (columnsToList (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))) table)
executeStatement (Select selectQ table) =
  case selectQ of 
  SelectColumns cols -> do 
    case doColumnsExist cols (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database)) of 
      True -> Right (createSelectDataFrame
                    (fst (getColumnsRows cols (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))))
                    (snd (getColumnsRows cols (fromMaybe (DataFrame [] []) (lookup table InMemoryTables.database))))
                    )
      False -> Left "Provided column name does not exist in database"
  SelectAggregate (Aggregate aggF colN) -> do
    case validateDataFrame (fromMaybe (DataFrame [] []) (findTableByName InMemoryTables.database table)) of  -- removinu maybe dedama i list ir td tvarkau, jei turit lengvesni buda - plz use
      True -> case aggF of
        Sum -> do 
          Left "Sum"
        Max -> do
          Left "Max"
      False -> Left "Can not show this table as it is invalid in database"
executeStatement _ = Left "Not implemented: executeStatement for other statements"
---------------------------------------------------------------------------------
-- might need to delete later (check only after everything is done)

validateDataFrame :: DataFrame -> Bool
validateDataFrame dataFrame
  | not (checkRowSizes dataFrame) = False
  | not (checkTupleMatch (zipColumnsAndValues dataFrame)) = False
  | otherwise = True

checkTupleMatch :: [(Column, Value)] -> Bool
checkTupleMatch [] = True  -- Base case when the list is empty
checkTupleMatch ((column, value) : rest) =
   case (column, value) of
    (Column _ IntegerType, IntegerValue _) -> checkTupleMatch rest
    (Column _ StringType, StringValue _) -> checkTupleMatch rest
    (Column _ BoolType, BoolValue _) -> checkTupleMatch rest
    _ -> False  -- Match any other case

zipColumnsAndValues :: DataFrame -> [(Column, Value)]
zipColumnsAndValues (DataFrame columns rows) = [(col, val) | row <- rows, (col, val) <- zip columns row]

checkRowSizes :: DataFrame -> Bool
checkRowSizes (DataFrame columns rows) = all (\row -> length row == length columns) rows

---------------------------------------------------------------------------------

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
        ("", _) -> Left $ "Expected whitespace before " ++  query
        (rest, whitespace) -> Right (rest, whitespace)

-------------------------------------------------------------------------------------

showTablesParser :: Parser ParsedStatement
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure ShowTables

------------------------------------------------------------------------------------

showTableParser :: Parser ParsedStatement
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- optional whitespaceParser
    ShowTable <$> tableNameParser

tableNameParser :: Parser TableName
tableNameParser = Parser $ \query ->
  case isValidTableName query of
    True ->
      case lookup (dropWhiteSpaces (init query)) InMemoryTables.database of
      Just _ -> Right (init (dropWhiteSpaces query), ";")
      Nothing -> Left "Table not found in the database or not provided"
    False -> Left "Query does not end with ; or contains unnecessary words after table name"

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
  | x /= ' ' = [x] ++ dropWhiteSpaces xs
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
    Select specialSelect <$> tableNameParser

selectDataParser :: Parser SpecialSelect
selectDataParser = tryParseAggregate <|> tryParseColumn
  where
    tryParseAggregate = do
        SelectAggregate <$> aggregateParser
    tryParseColumn = do
        SelectColumns <$> columnNamesParser

aggregateParser :: Parser Aggregate
aggregateParser = do
    func <- aggregateFunctionParser
    _ <- optional whitespaceParser
    _ <- char '('
    _ <- optional whitespaceParser
    columnName <- columnNameParser
    _ <- optional whitespaceParser
    _ <- char ')'
    pure $ Aggregate func columnName

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
  case isOneWord' query of
    True -> Right (fst (splitStatementAtParentheses query), snd (splitStatementAtParentheses query))
    False -> Left ("There is more than one column name in aggregation function or ')' is missing")

isOneWord' :: String -> Bool
isOneWord' (x:xs)
  | x == ',' = False
  | x == ' ' = isOneWord' xs
  | x == ')' = True
  | otherwise = isOneWord' xs

splitStatementAtParentheses :: String -> (String, String)
splitStatementAtParentheses = go [] where
  go _ [] = ("", "")
  go prefix str@(x:xs)
    | ")" `isPrefixOf` toLowerString str = (reverse prefix, str)
    | otherwise = go (x:prefix) xs

columnNamesParser :: Parser [ColumnName]
columnNamesParser = Parser $ \query ->
  case query == "" || (dropWhiteSpaces query) == ";" of
    True -> Left "Column name is expected"
    False -> case toLowerString (head (split query ' ')) == "from" of
      True -> Left "No column name was provided"
      False -> case commaBetweenColumsNames (fst (splitStatementAtFrom query)) && areColumnsListedRight (fst (splitStatementAtFrom query)) && areColumnsListedRight (snd (splitStatementAtFrom query)) of
        True -> Right ((split (dropWhiteSpaces (fst (splitStatementAtFrom query))) ','), snd (splitStatementAtFrom query))
        False -> Left "Column names are not listed right or from is missing"

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
      if x `elem` dfColumnNames
      then doColumnsExist xs df
      else False

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

---------------------------------------------------------------------------------------------
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
getColumnList (x:xs) (y:ys) = [(Column x y)] ++ getColumnList xs ys

findColumnIndex :: ColumnName -> [Column] -> Int
findColumnIndex columnName columns = columnIndex columnName columns 0

columnIndex :: ColumnName -> [Column] -> Int -> Int
columnIndex columnName ((Column name _):xs) index
    | columnName /= name = columnIndex columnName xs (index + 1)
    | otherwise = index

---------------------------------------------------------------------------------------------------------------

toLowerString :: String -> String
toLowerString [] = ""
toLowerString (x:xs) = charToString (toLower x) ++ toLowerString xs

charToString :: Char -> String
charToString c = [c]

----------------------------------------------------------------------------------------------------------

createColumnsDataFrame :: [ColumnName] -> TableName -> DataFrame
createColumnsDataFrame columnNames columnTableName = DataFrame [Column columnTableName StringType] (map (\name ->  [StringValue name]) columnNames)

createSelectDataFrame :: [Column] -> [Row] -> DataFrame
createSelectDataFrame columns rows = DataFrame columns rows

createTablesDataFrame :: [TableName] -> DataFrame
createTablesDataFrame tableNames = DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

---------------------------------------------------------------------------------------------------------

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
