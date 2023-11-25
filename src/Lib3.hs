{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}

module Lib3
  ( executeSql,
    parseStatement,
    Execution,
    ParsedStatement2,
    ExecutionAlgebra(..)
  )
where
import Lib2
import Lib1 (renderDataFrameAsTable, findTableByName, parseSelectAllStatement, checkTupleMatch, zipColumnsAndValues, checkRowSizes)
import Control.Monad.Free (Free (..), liftF)
import DataFrame
import Data.Time ( UTCTime )
import Data.Either (fromRight)
import Text.ParserCombinators.ReadP (many1)
import GHC.Generics
import Data.Aeson
import DataFrame as DF
import GHC.Base (VecElem(DoubleElemRep))
 
type TableName = String
type FileContent = String
type DeserializedContent = Either ErrorMessage (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTables (TableArray -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors heres
  deriving Functor

type Execution = Free ExecutionAlgebra
 
--instance FromJSON FromJSONTable

-- Keep the type, modify constructors
data ParsedStatement2 =
  SelectNow {}
  | Insert {
    table :: TableName,
    columns :: Maybe SelectedColumns,
    values :: InsertedValues
  }
  | Update {
    table :: TableName,
    selectUpdate :: Maybe WhereSelect,
    selectWhere :: Maybe WhereSelect
  }
  | Delete {
    table :: TableName,
    conditions :: Maybe WhereSelect
  }
  | ShowTable {
    table :: TableName
   }
  |SelectAll {
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
   }
  |Select {
    selectQuery :: SpecialSelect,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
  }
  | ShowTables { }
    deriving (Show, Eq)

data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [DF.Value]

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getTables :: Execution TableArray
getTables = liftF $ GetTables id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement2 sql of
  Right (SelectAll tables selectWhere) -> do
    let df = [DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]], DataFrame [Column "value" BoolType][[BoolValue True],[BoolValue True],[BoolValue False]]]
    case executeSelectAll df selectWhere of
      Right dfs -> return $ Right dfs
      Left err -> return $ Left err
  Left err -> return $ Left err
  --return $ Left "implement me" 


  --EXECUTE SELECT ALL
  -- let dfs = [DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]], DataFrame [Column "value" BoolType][[BoolValue True],[BoolValue True],[BoolValue False]]]
  -- case executeSelectAll dfs Nothing of
  --   Right df -> return $ Right df
  --   Left err -> return $ Left err

  --EXECUTE SHOW TABLE
  -- file <- loadFile "flags"
  -- let df = executeShowTable (DataFrame [] []) "flags" --dataframe ideti is loadFile gauta
  -- return $ Right df

  --EXECUTE SHOW TABLES
  --files <- getTables
  --let df = executeShowTables files
  --return $ Right df

-------------------------------some JSON shit------------------------------------

toJSONtable :: (TableName, (DataFrame)) -> String
toJSONtable table = "{\"Table\":\"" ++ show(fst(table)) ++ "\",\"Columns\":[" ++ show(toJSONColumns(toColumnList(snd(table)))) ++ "],\"Rows\":[" ++ show(toJSONRows(toRowList(snd(table)))) ++"]}"

toJSONColumn :: Column -> String
toJSONColumn column = "{\"Name\":\"" ++ show(getColumnName(column)) ++ ",\"ColumnType\":\"" ++ show(getType(column)) ++ "\"}"

toJSONRowValue :: DF.Value -> String
toJSONRowValue value = "{\"Value\":\"" ++ show(value) ++ "\"}"

---------------------------------------some get stuff----------------------------

toColumnList :: DataFrame -> [Column]
toColumnList (DataFrame col row)  = col

toRowList :: DataFrame -> [Row]
toRowList (DataFrame co row) = row

----------------------------------recursive stuff-------------------------------

toJSONColumns :: [Column] -> String
toJSONColumns (x:xs)
  |xs /= [] = toJSONColumn x ++ "," ++ toJSONColumns xs
  |otherwise = toJSONColumn x

toJSONRow :: Row -> String
--toJSONRow [] = []
toJSONRow (x:xs) 
  |xs /= [] = toJSONRowValue x ++ "," ++ toJSONRow xs
  |otherwise = toJSONRowValue x

toJSONRows :: [Row] -> String
toJSONRows (x:xs)
  |xs /= [] = "[" ++ toJSONRow x ++ "]," ++ toJSONRows xs
  |otherwise = "[" ++ toJSONRow x ++ "]" 

--------------------------------Files--------------------------------------------

toFilePath :: TableName -> FilePath
toFilePath tableName = "db/" ++ show(tableName) ++ ".json" --".txt"

writeTableToFile :: (TableName, DataFrame) -> IO () 
writeTableToFile table = writeFile (toFilePath(fst(table))) (toJSONtable(table))

---------------------------------------------------------------------------------

parseStatement2 :: String -> Either ErrorMessage ParsedStatement2
parseStatement2 query = case runParser p query of
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
        Insert _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Update _ _ _-> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Delete _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        SelectNow -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
    where
        p :: Parser ParsedStatement2
        p = showTableParser
               <|> showTablesParser
               <|> selectStatementParser
               <|> selectAllParser
               <|> insertParser
               <|> updateParser
               <|> deleteParser
               <|> selectNowParser

--------------------------------------------------------------------------------

executeShowTable :: DataFrame -> TableName -> DataFrame
executeShowTable df table = createColumnsDataFrame (columnsToList df) table

executeShowTables :: TableArray -> DataFrame
executeShowTables tables = createTablesDataFrame tables

executeSelectAll :: TableArray -> [DataFrame] -> Maybe WhereSelect -> Either ErrorMessage DataFrame
executeSelectAll tables selectedDfs whereSelect = case areTablesValid selectedDfs of
    True -> case whereSelect of
      Just conditions -> case isFaultyConditions conditions of
        False -> case doColumnsExistDFs (whereConditionColumnList conditions) selectedDfs of
          True -> case doColumnsExistProvidedDfs (whereConditionColumnList2 conditions) tables of
            True -> Left "do stuff"
            False -> Left "Some of provided columns do not exist in provided tables or expected table where not provided after 'from'"
          False -> Left "Some of provided columns do not exist in provided tables"
        True -> Left "Conditions are faulty"
      Nothing -> Right $ cartesianDataFrame selectedDfs
    False -> Left "Some of provided table(s) are not valid"

areTablesValid :: [DataFrame] -> Bool
areTablesValid [] = True
areTablesValid (x:xs)
  | validateDataFrame x = areTablesValid xs
  | otherwise = False
  
cartesianDataFrame :: [DataFrame] -> DataFrame
cartesianDataFrame [] = DataFrame [] [] 
cartesianDataFrame (df@(DataFrame cols rows):xs) = case getNextDataFrame xs of
  Nothing -> df
  Just nextDf -> cartesianDataFrame $ [DataFrame (cols ++ dcols) (cartesianProduct rows drows)] ++ rest
    where
      DataFrame dcols drows = nextDf
      rest = filter (/= nextDf) xs

cartesianProduct :: [[a]] -> [[a]] -> [[a]]
cartesianProduct xs ys = [x ++ y | x <- xs, y <- ys]

getNextDataFrame :: [DataFrame] -> Maybe DataFrame
getNextDataFrame [] = Nothing
getNextDataFrame (y:_) = Just y

doColumnsExistDFs :: [ColumnName] -> [DataFrame] -> Bool
doColumnsExistDFs _ [] = False
doColumnsExistDFs colNames dataFrames = all (\colName -> any (\df -> doColumnsExist [colName] df) dataFrames) colNames

whereConditionColumnList2 :: [Condition] -> [(TableName, ColumnName)]
whereConditionColumnList2 [] = []
whereConditionColumnList2 (x:xs) = whereConditionColumnName2 x ++ whereConditionColumnList2 xs

whereConditionColumnName2 :: Condition -> [(TableName, ColumnName)]
whereConditionColumnName2 (Condition op1 _ op2) =
  case op1 of
    ColumnTableOperand name1 -> case op2 of
      ColumnTableOperand name2 -> [name1] ++ [name2]
      _ -> [name1]
    ConstantOperand _ -> case op2 of
      ColumnTableOperand name -> [name]
      ConstantOperand _ -> []
---iki cia:
doColumnsExistProvidedDfs :: TableArray -> [DataFrame] -> [(TableName, ColumnName)] -> Either ErrorMessage Bool
doColumnsExistProvidedDfs ta df (x:xs)
  | 

checkIfConditionsMatchesWithData :: TableArray -> [DataFrame] -> (TableName, ColumnName) -> Bool
checkIfConditionsMatchesWithData [] [] _ = False
checkIfConditionsMatchesWithData (x:xs) (y:ys) (table, column)
  | x == table && doColumnsExist [column] y == true = True
  | otherwise = checkIfConditionsMatchesWithData xs ys (table, column)
--------------------------------------------------------------------------------

selectNowParser :: Parser ParsedStatement2
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow

insertParser :: Parser ParsedStatement2
insertParser = do
    _ <- queryStatementParser "insert"
    _ <- whitespaceParser
    _ <- queryStatementParser "into"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    selectUpdate <- optional selectDataParsers
    _ <- queryStatementParser ")"
    _ <- queryStatementParser "values"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    values <- insertedValuesParser
    _ <- queryStatementParser ")"
    pure $ Insert tableName selectUpdate values

insertedValuesParser :: Parser InsertedValues
insertedValuesParser = do
  values <- sepBy constantParser (char ',' >> whitespaceParser)
  return values

sepBy :: Parser a -> Parser b -> Parser [a]
sepBy p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

deleteParser :: Parser ParsedStatement2
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- optional whitespaceParser
    _ <- queryStatementParser "where"
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser ParsedStatement2
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- whitespaceParser
    _ <- queryStatementParser "set"
    selectUpdated <- optional setParserWithComma
    _ <- whitespaceParser
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParserWithComma :: Parser WhereSelect
setParserWithComma = do
  _ <- whitespaceParser
  some setCommaExist

  where
    setCommaExist :: Parser Condition
    setCommaExist = do
      condition <- whereConditionParser
      _ <- optional (queryStatementParser "," >> whitespaceParser)
      pure condition

selectDataParsers :: Parser SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ ColumnsSelected columnNames

----Lib2 stuff----
selectAllParser :: Parser ParsedStatement2
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
  pure $ SelectAll tableArray selectWhere

selectStatementParser :: Parser ParsedStatement2
selectStatementParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    specialSelect <- selectDataParser
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableArray <- selectTablesParser
    _ <- optional whitespaceParser
    selectWhere <- optional whereParser
    _ <- optional whitespaceParser
    pure $ Select specialSelect tableArray selectWhere

selectDataParser :: Parser SpecialSelect
selectDataParser = tryParseAggregate <|> tryParseColumn <|> tryParseColumnTable
  where
    tryParseAggregate = do
      aggregateList <- seperate aggregateParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectAggregate aggregateList
    tryParseColumn = do
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectColumns columnNames
    tryParseColumnTable = do
      columnsWithTables <- seperate columnNameTableParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectColumnsTables columnsWithTables

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
               <|> (ColumnTableOperand <$> columnNameTableParser)
               <|> (ColumnOperand <$> columnNameParser)

columnNameTableParser :: Parser (TableName, ColumnName)
columnNameTableParser = do
  table <- columnNameParser
  _ <- char '.'
  column <- columnNameParser
  return (table, column)

-------------------------------------------------------------------------------

selectTablesParser :: Parser TableArray
selectTablesParser = tryParseTable
  where
    tryParseTable = do
      tableNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      return $ tableNames

showTablesParser :: Parser ParsedStatement2
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser ParsedStatement2
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    pure $ ShowTable table
