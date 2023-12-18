{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}

module Lib4
  (
    ParsedStatement3, 
    parseStatement,
    SqlStatement(..), 
    fromStatement,
    toTable,
    toDataframe,
    SqlException(..),
    SqlTableFromYAML(..),
    fromException,
    toStatement,
    ValueFromYAML,
    RowFromYAML,
    ColumnFromYAML,
    fromTable,
    fromDataFrame
  ) 
where

import Lib2(dropWhiteSpaces, getOperand, stringToInt, isNumber, areSpacesBetweenWords, splitStatementAtParentheses, tableNameParser)
import Control.Applicative(Alternative(empty, (<|>)),optional, some, many)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT, state)
import Data.Char (toLower, GeneralCategory (ParagraphSeparator), isSpace, isAlphaNum, isDigit, digitToInt)
import Data.String (IsString, fromString)
import Control.Monad.Trans.Class
import Control.Monad.Trans.State.Strict
import DataFrame as DF
import Data.Char (isDigit)
import GHC.IO.Handle (NewlineMode(inputNL))
import Data.List (isPrefixOf, nub)
import Data.Yaml
import GHC.Generics
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL


type ColumnName = String
type Error = String
type Parser4 a = EitherT Error (State String) a

data AscDesc = Asc String | Desc String
  deriving (Show, Eq)

data OrderByValue = ColumnTable (TableName, ColumnName) | ColumnName ColumnName | ColumnNumber Integer 
  deriving (Show, Eq)

type OrderBy = [(OrderByValue, AscDesc)]

type TableName = String 
type FileContent = String
type DeserializedContent = (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data Trash = Trash String
  deriving (Show, Eq)

data NowFunction = Now
  deriving (Show, Eq)

data Aggregate2 = AggregateColumn (AggregateFunction, ColumnName) | AggregateColumnTable (AggregateFunction, (TableName, ColumnName))
  deriving (Show, Eq)

data SpecialSelect2 = SelectAggregate2 [Aggregate2] (Maybe NowFunction) | SelectColumn2 [ColumnName] (Maybe NowFunction) | SelectedColumnsTables [(TableName, ColumnName)] (Maybe NowFunction)
  deriving (Show, Eq)

data AggregateFunction = Sum | Max
  deriving (Show, Eq)

data And = And
  deriving (Show, Eq)

type AggregateList = [(AggregateFunction, ColumnName)]

data SpecialSelect = SelectAggregate AggregateList | SelectColumns [ColumnName]
  deriving (Show, Eq)

data Operand = ColumnOperand ColumnName | ConstantOperand DF.Value | ColumnTableOperand (TableName, ColumnName)
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

data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [DF.Value]

data ParsedStatement3 =
  SelectNow {}
  | DropTable {
    table :: TableName
  }
  | CreateTable {
    table :: TableName,
    newColumns :: [Column]
  }
  | Insert {
    table :: TableName,
    columns :: Maybe SelectedColumns,
    values :: InsertedValues
  }
  | Update {
    table :: TableName,
    selectUpdate :: WhereSelect,
    selectWhere :: Maybe WhereSelect
  }
  | Delete {
    table :: TableName,
    conditions :: Maybe WhereSelect
  }
  | ShowTable {
    table :: TableName
   }
  | SelectAll {
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect,
    order :: Maybe OrderBy
   }
  |Select {
    selectQuery :: SpecialSelect2,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect,
    order :: Maybe OrderBy
  }
  | ShowTables { }
    deriving (Show, Eq)


------------------for communication starts-------------
data SqlStatement = SqlStatement {
  statement :: String
} deriving (Generic, Show)

data SqlException = SqlException {
  exception :: String
} deriving (Generic)

data SqlTableFromYAML = SqlTableFromYAML {
  columnsYAML :: ColumnsFromYAML,
  rowsYAML :: RowsFromYAML
} deriving (Generic)

data ColumnsFromYAML = ColumnsFromYAML{
  columnListYAML :: [ColumnFromYAML]
  } deriving (Generic)

data ColumnFromYAML = ColumnFromYAML{
  columnNameYAML :: String,
  columnTypeYAML :: String
} deriving (Generic)

data RowsFromYAML = RowsFromYAML{
  rowListYAML :: [RowFromYAML]
  } deriving (Generic)

data RowFromYAML = RowFromYAML{
  rowYAML :: [ValueFromYAML]
} deriving (Generic)

data ValueFromYAML = ValueFromYAML{
  valueYAML :: String
} deriving (Generic)

--------------------------------------------
--patikrinto instances kai nebereks Data.Yaml, nes tada matysis ar fromJSON ar fromYAML palaiko ir kurios bybles pasiekia (man is kazkur aeson traukia) 
instance FromJSON SqlException
instance FromJSON SqlStatement
instance FromJSON SqlTableFromYAML
instance FromJSON ColumnsFromYAML
instance FromJSON RowsFromYAML
instance FromJSON ColumnFromYAML
instance FromJSON RowFromYAML
instance FromJSON ValueFromYAML


instance ToJSON SqlException
instance ToJSON SqlStatement
instance ToJSON SqlTableFromYAML
instance ToJSON ColumnsFromYAML
instance ToJSON RowsFromYAML
instance ToJSON ColumnFromYAML
instance ToJSON RowFromYAML
instance ToJSON ValueFromYAML
--------------------------------------------
-- also no clue ar tikra taip
toTable :: String -> Maybe SqlTableFromYAML
toTable yasm = decode $ BS.pack yasm

toStatement :: String -> Maybe SqlStatement
toStatement yasm = decode $ BS.pack yasm

toException :: String -> Maybe SqlException
toException yasm = decode $ BS.pack yasm

fromTable :: SqlTableFromYAML -> String
fromTable table = BS.unpack (encode table)

fromStatement :: SqlStatement -> String
fromStatement statement = BS.unpack (encode statement)

fromException :: SqlException -> String
fromException exception = BS.unpack (encode exception)


toDataframe :: SqlTableFromYAML -> DataFrame
toDataframe table =
  DataFrame
    (map (\col -> Column (columnNameYAML col) (toColumnType $ columnTypeYAML col)) $ columnListYAML $ columnsYAML table)
    (map (map convertToValue . rowYAML) $ rowListYAML $ rowsYAML table)


toColumnType :: String -> ColumnType
toColumnType "IntegerType" = IntegerType
toColumnType "StringType"  = StringType
toColumnType "BoolType"    = BoolType
toColumnType _         = error "Unsupported data type"

convertToValue :: ValueFromYAML -> DF.Value
convertToValue (ValueFromYAML str) =
  case words str of
    ["IntegerValue", val] -> IntegerValue (read val)
    ["StringValue", val] -> StringValue val
    ["BoolValue", "True"] -> BoolValue True
    ["BoolValue", "False"] -> BoolValue False
    ["NullValue"] -> NullValue
    _ -> error "Invalid FromJSONValue format"

fromDataFrame :: DataFrame -> SqlTableFromYAML
fromDataFrame (DataFrame col row) = SqlTableFromYAML {
  columnsYAML = fromColumns col,
  rowsYAML = fromRows row
}

fromColumns :: [Column] -> ColumnsFromYAML
fromColumns [] = ColumnsFromYAML { columnListYAML = [] }
fromColumns (x:xs) = ColumnsFromYAML {
  columnListYAML = fromColumn x : columnListYAML (fromColumns xs)
}

fromColumn :: Column -> ColumnFromYAML
fromColumn (Column name colType) = ColumnFromYAML {
  columnNameYAML = name,
  columnTypeYAML = show colType
}

fromRows :: [Row] -> RowsFromYAML
fromRows [] = RowsFromYAML {rowListYAML = []}
fromRows (x:xs) = RowsFromYAML {
  rowListYAML = fromRow x : rowListYAML (fromRows xs)
}


fromRow :: Row -> RowFromYAML
fromRow [] = RowFromYAML { rowYAML = [] }
fromRow (x:xs) = RowFromYAML {
  rowYAML = fromValue x : rowYAML (fromRow xs)
}


fromValue :: DF.Value -> ValueFromYAML
fromValue value = ValueFromYAML{
  valueYAML = show value
}

-----------------for communication ends----------------



newtype EitherT e m a = EitherT {
    runEitherT :: m (Either e a)
}


instance MonadTrans (EitherT e) where
  lift :: Monad m => m a -> EitherT e m a
  lift ma = EitherT $ fmap Right ma

instance Monad m => Functor (EitherT e m) where
  fmap :: (a -> b) -> EitherT e m a -> EitherT e m b
  fmap f ta = EitherT $ do
    eit <- runEitherT ta
    case eit of
      Left e -> return $ Left e
      Right a -> return $ Right (f a)

instance Monad m => Applicative (EitherT e m) where
  pure :: a -> EitherT e m a
  pure a = EitherT $ return $ Right a
  (<*>) :: EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
  af <*> aa = EitherT $ do
    f <- runEitherT af
    case f of
      Left e1 -> return $ Left e1
      Right r1 -> do
        a <- runEitherT aa
        case a of
          Left e2 -> return $ Left e2
          Right r2 -> return $ Right (r1 r2)

instance Monad m => Monad (EitherT e m) where
  (>>=) :: EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
  m >>= k = EitherT $ do
    eit <- runEitherT m
    case eit of
      Left e -> return $ Left e
      Right r -> runEitherT (k r)

throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err

parseStatement :: String -> Either Error ParsedStatement3
parseStatement input = do
  (query, remain) <- runParser p input
  (_,_) <- runParser stopParseAt remain
  return query
  where
    p :: Parser4 ParsedStatement3
    p = showTableParser
               <|> showTablesParser
               <|> selectStatementParser
               <|> selectAllParser
               <|> insertParser
               <|> updateParser
               <|> deleteParser
               <|> selectNowParser
               <|> createTableParser
               <|> dropTableParser

dropTableParser :: Parser4 ParsedStatement3
dropTableParser = do
    _ <- queryStatementParser "drop"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser 
    pure $ DropTable table

createTableParser :: Parser4 ParsedStatement3
createTableParser = do
    _ <- queryStatementParser "create"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- whitespaceParser
    _ <- char '('
    _ <- optional whitespaceParser
    columnsAndTypes <- columnListParser
    _ <- optional whitespaceParser
    _ <- char ')'
    _ <- optional whitespaceParser
    pure $ CreateTable table columnsAndTypes

runParser :: Parser4 a -> String -> Either Error (a, String)
runParser parser input = 
    let eitherResult = runState (runEitherT parser) input
    in case eitherResult of
        (Left errMsg, _) -> Left errMsg
        (Right value, remainingInput) -> Right (value, remainingInput)

instance (IsString e) => Alternative (EitherT e (State s)) where
    empty :: EitherT e (State s) a
    empty = EitherT $ state $ \s -> (Left (fromString "Error"), s)

    (<|>) :: EitherT e (State s) a -> EitherT e (State s) a -> EitherT e (State s) a
    a1 <|> a2 = EitherT $ state $ \s ->
        let result = runState (runEitherT a1) s
        in case result of
            (Left _, _) -> runState (runEitherT a2) s
            _ -> result

----------------------------------------------------------------------------------

insertParser :: Parser4 ParsedStatement3
insertParser = do
    _ <- queryStatementParser "insert"
    _ <- whitespaceParser
    _ <- queryStatementParser "into"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- optional whitespaceParser
    selectUpdate <- optional selectDataParsers
    _ <- optional whitespaceParser
    _ <- queryStatementParser "values"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    values <- insertedValuesParser
    _ <- queryStatementParser ")"
    pure $ Insert tableName selectUpdate values

selectDataParsers :: Parser4 SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      _ <- queryStatementParser "("
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- queryStatementParser ")"
      return $ ColumnsSelected columnNames

insertedValuesParser :: Parser4 InsertedValues
insertedValuesParser = do
  values <- seperate valueParser (queryStatementParser "," >> optional whitespaceParser)
  return values

valueParser :: Parser4 DF.Value
valueParser = parseNullValue <|> parseBoolValue <|> parseStringValue <|> parseNumericValue

parseNullValue :: Parser4 DF.Value
parseNullValue = queryStatementParser "null" >> return NullValue

parseBoolValue :: Parser4 DF.Value
parseBoolValue =
  (queryStatementParser "true" >> return (BoolValue True))
  <|> (queryStatementParser "false" >> return (BoolValue False))

parseStringValue :: Parser4 DF.Value
parseStringValue = do
  strValue <- parseStringWithQuotes
  return (StringValue strValue)

parseStringWithQuotes :: Parser4 String
parseStringWithQuotes = do
  _ <- queryStatementParser "'"
  str <- some (parseSatisfy (/= '\''))
  _ <- queryStatementParser "'"
  return str

parseNumericValue :: Parser4 DF.Value
parseNumericValue = IntegerValue <$> parseInt

parseInt :: Parser4 Integer
parseInt = do
  digits <- some (parseSatisfy isDigit)
  return (read digits)

deleteParser :: Parser4 ParsedStatement3
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser4 ParsedStatement3
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
    selectUpdated <- setParser
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParser :: Parser4 WhereSelect
setParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "set"
  _ <- whitespaceParser
  seperate whereConditionParser (optional whitespaceParser >> char ',' >> optional whitespaceParser)

selectAllParser :: Parser4 ParsedStatement3
selectAllParser = do
  _ <- queryStatementParser "select"
  _ <- whitespaceParser
  _ <- queryStatementParser "*"
  _ <- whitespaceParser
  _ <- queryStatementParser "from"
  _ <- whitespaceParser
  tableArray <- selectTablesParser
  selectWhere <- optional whereParser
  _ <- optional whitespaceParser
  orderBy <- optional orderByParser
  _ <- optional whitespaceParser
  pure $ SelectAll tableArray selectWhere orderBy

--------------------------order by----------------------------------
orderByParser :: Parser4 OrderBy
orderByParser = do
  some orderValuesParser
  where 
    orderValuesParser :: Parser4 (OrderByValue, AscDesc)
    orderValuesParser = do
      value <- orderByValueParser
      ascDesc <- ascDescParser
      _ <- optional (whitespaceParser >> char ',')
      pure (value, ascDesc)

orderByValueParser :: Parser4 OrderByValue
orderByValueParser = do (ColumnTable <$> columnNameTableParser) <|> (ColumnName <$> columnNameParser) <|> (ColumnNumber <$> numberParser)


ascDescParser :: Parser4 AscDesc
ascDescParser = tryParseDesc <|> tryParseAsc <|> tryParseSymbol
  where
    tryParseDesc = do
      _ <- whitespaceParser
      _ <- queryStatementParser "desc"
      return $ Desc "desc"
    tryParseAsc = do
      _ <- whitespaceParser
      _ <- queryStatementParser "asc"
      return $ Asc "asc"
    tryParseSymbol = do
      _ <- optional whitespaceParser
      return $ Asc "asc"

-----------------------------------------------------------------

selectNowParser :: Parser4 ParsedStatement3
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow

selectStatementParser :: Parser4 ParsedStatement3
selectStatementParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    specialSelect <- selectDataParser
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableArray <- selectTablesParser
    selectWhere <- optional whereParser
    _ <- optional whitespaceParser
    orderBy <- optional orderByParser
    _ <- optional whitespaceParser
    pure $ Select specialSelect tableArray selectWhere orderBy

selectDataParser :: Parser4 SpecialSelect2
selectDataParser = tryParseAggregate <|>  tryParseColumn <|> tryParseColumnTable
  where
    tryParseAggregate = do
      nowf <- optional nowParser
      aggregateList <- seperate aggregateParser' (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectAggregate2 aggregateList nowf
    tryParseColumn = do
      nowf <- optional nowParser
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectColumn2 columnNames nowf
    tryParseColumnTable = do
      nowf <- optional nowParser
      columnsWithTables <- seperate columnNameTableParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectedColumnsTables columnsWithTables nowf

aggregateParser' :: Parser4 Aggregate2
aggregateParser' = tryColumnTables <|> tryColumns
  where 
    tryColumns = do
      func <- aggregateFunctionParser
      _ <- optional whitespaceParser
      _ <- char '('
      _ <- optional whitespaceParser
      columnName <- columnNameParser''
      _ <- optional whitespaceParser
      _ <- char ')'
      return $ AggregateColumn (func, columnName)
    tryColumnTables = do
      func <- aggregateFunctionParser
      _ <- optional whitespaceParser
      _ <- char '('
      _ <- optional whitespaceParser
      columnName <- columnNameTableParser
      _ <- optional whitespaceParser
      _ <- char ')'
      return $ AggregateColumnTable (func, columnName)

nowParser :: Parser4 NowFunction
nowParser = do
  _ <- optional whitespaceParser
  _ <- queryStatementParser "now"
  _ <- optional whitespaceParser
  _ <- queryStatementParser "("
  _ <- optional whitespaceParser
  _ <- queryStatementParser ")"
  _ <- optional whitespaceParser
  _ <- queryStatementParser ","
  _ <- optional whitespaceParser
  pure Now

whereParser :: Parser4 WhereSelect
whereParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "where"
  _ <- whitespaceParser
  some whereAndExist
  where
    whereAndExist :: Parser4 Condition
    whereAndExist = do
      condition <- whereConditionParser
      _ <- optional (whitespaceParser >> andParser)
      pure condition

andParser :: Parser4 And
andParser = queryStatementParser "and" >> pure And

whereConditionParser :: Parser4 Condition
whereConditionParser = do
  _ <- optional whitespaceParser
  operand1 <- operandParser
  _ <- optional whitespaceParser
  operator <- operatorParser
  _ <- optional whitespaceParser
  operand2 <- operandParser
  return $ Condition operand1 operator operand2

operandParser :: Parser4 Operand
operandParser = (ConstantOperand <$> constantParser)
               <|> (ColumnTableOperand <$> columnNameTableParser)
               <|> (ColumnOperand <$> columnNameParser)

columnNameTableParser :: Parser4 (TableName, ColumnName)
columnNameTableParser = do
  table <- columnNameParser
  _ <- char '.'
  column <- columnNameParser
  return (table, column)

-------------------------------------------------------------------------------

selectTablesParser :: Parser4 TableArray
selectTablesParser = tryParseTable
  where
    tryParseTable = do
      tableNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      return $ tableNames

showTablesParser :: Parser4 ParsedStatement3
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure Lib4.ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser4 ParsedStatement3
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    pure $ Lib4.ShowTable table

columnListParser :: Parser4 [Column]
columnListParser = seperate columnAndTypeParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)

columnAndTypeParser :: Parser4 Column
columnAndTypeParser = do
    columnName <- columnNameParser
    _ <- whitespaceParser
    columnType <- columnNameParser >>= either (throwE . show) pure . columnTypeParser
    pure (Column columnName columnType)

columnTypeParser :: String -> Either Error ColumnType
columnTypeParser "int" = Right IntegerType
columnTypeParser "varchar" = Right StringType
columnTypeParser "bool" = Right BoolType
columnTypeParser other = Left $ "There is no such type as: " ++ other

--sitas gali ir neveikti:
parseSatisfy :: (Char -> Bool) -> Parser4 Char
parseSatisfy pr = EitherT $ state $ \input ->
  case input of
    (x:xs) -> if pr x then (Right x, xs) else (Left ("Unexpected character: " ++ [x]), input)
    [] -> (Left "Empty input", input)

--sitas gali ir neveikti:
queryStatementParser :: String -> Parser4 String
queryStatementParser stmt = do
  input <- lift get
  case take (length stmt) input of
    [] -> throwE "Expected ;"
    xs
      | map toLower xs == map toLower stmt -> do
        lift $ put $ drop (length xs) stmt
        return xs
      | otherwise -> throwE $ "Expected " ++ stmt ++ " or query contains unnecessary words"

whitespaceParser :: Parser4 String
whitespaceParser = do
  input <- lift get
  case span isSpace input of
    ("",_) -> throwE $ "Expected whitespace before: " ++ input
    (ws, remain) -> do
      lift $ put remain
      return ws

seperate :: Parser4 a -> Parser4 b -> Parser4 [a]
seperate p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

columnNameParser :: Parser4 ColumnName
columnNameParser = do
  input <- lift get
  case takeWhile (\x -> isAlphaNum x || x == '_') input of
    [] -> throwE "Empty input"
    xs -> do
      lift $ put $ drop (length xs) input
      return xs

char :: Char -> Parser4 Char
char c = do
  input <- lift get
  case input of
    [] -> throwE "Empty input"
    (x:xs) -> if x == c then do 
                            lift $ put xs 
                            return c
                        else throwE ("Expected " ++ [c])

numberParser :: Parser4 Integer
numberParser = do
  input <- lift get
  case takeWhile (\x -> isDigit x) input of
    [] -> throwE "Empty input"
    xs -> do
      lift $ put $ drop (length xs) input
      return $ stringToInt xs

trashParser :: Parser4 Trash 
trashParser = do
  input <- lift get
  case head input == ',' of 
    True -> throwE "Columns are not listed right"
    False -> case "from" `isPrefixOf` (dropWhiteSpaces input) of 
      True -> do
                lift $ put input
                return $ Trash ""
      False -> throwE "Columns are not listed right or aggregate functions and column names cannot be mixed"

constantParser :: Parser4 DF.Value
constantParser = do
  input <- lift get
  case input == "" of 
    True -> throwE "The query does not end with a ;"
    False -> 
      let operand = getOperand input
          restQuery = drop (length operand) input
      in case head input == ';' of
          False -> case head operand == '\'' && last operand == '\'' of
            True -> do
                      lift $ put restQuery
                      return $ StringValue (init (tail operand))
            False -> case operand of
              "True" -> do
                          lift $ put restQuery
                          return $ BoolValue True
              "False" -> do
                          lift $ put restQuery
                          return $ BoolValue False
              "null" -> do
                          lift $ put restQuery
                          return $ NullValue
              _ ->  if isNumber operand
                    then do 
                            lift $ put restQuery
                            return $ IntegerValue $ stringToInt operand
                    else throwE "Operand is not valid"
          True -> throwE "The conditions are missing"


columnNameParser' :: Parser4 ColumnName
columnNameParser' = do
  input <- lift get
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses input)) 
    then do
            lift $ put $ snd (splitStatementAtParentheses input) 
            return $ dropWhiteSpaces (fst (splitStatementAtParentheses input))
  else throwE "There is more than one column name in aggregation function")


columnNameParser'' :: Parser4 ColumnName
columnNameParser'' = do
  input <- lift get
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses input)) 
    then do
            lift $ put $ snd (splitStatementAtParentheses input)
            return $ dropWhiteSpaces (fst (splitStatementAtParentheses input))
  else throwE "There is more than one column name in aggregation function")

operatorParser :: Parser4 Operator
operatorParser = 
  (queryStatementParser "=" >> pure IsEqualTo)
  <|> (queryStatementParser "!=" >> pure IsNotEqual)
  <|> (queryStatementParser "<" >> pure IsLessThan)
  <|> (queryStatementParser ">" >> pure IsGreaterThan)
  <|> (queryStatementParser "<=" >> pure IsLessOrEqual)
  <|> (queryStatementParser ">=" >> pure IsGreaterOrEqual)

aggregateFunctionParser :: Parser4 AggregateFunction
aggregateFunctionParser = sumParser <|> maxParser
  where
    sumParser = do
        _ <- queryStatementParser "sum"
        pure Sum
    maxParser = do
        _ <- queryStatementParser "max"
        pure Max

stopParseAt :: Parser4 String
stopParseAt  = do
  _ <- optional whitespaceParser
  _ <- queryStatementParser ";"
  checkAfterQuery
  where
    checkAfterQuery :: Parser4 String
    checkAfterQuery = do
      query <- lift get
      case query of
          [] -> do
                  lift $ put []
                  return []
          s -> throwE ("Characters found after ;" ++ s)