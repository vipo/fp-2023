{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# HLINT ignore "Redundant pure" #-}

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
import Data.Time
import Data.Either (fromRight)
import Data.List (find, findIndex, elemIndex)
import Text.ParserCombinators.ReadP (many1, sepBy1)
import GHC.Generics
import GHC.Generics
import Data.Time
import GHC.Base (VecElem(DoubleElemRep))
import Debug.Trace (trace, traceShow)
import Data.Char (isDigit)
import Data.Maybe (fromMaybe)
import Control.Monad (filterM, liftM)
import Control.Monad (sequence)
import Data.List (findIndex)
import Data.Maybe (fromMaybe)
import Data.Time.Clock (UTCTime)
import Data.List (findIndex)

type TableName = String
type FileContent = String
type DeserializedContent = (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors heres
  deriving Functor

type Execution = Free ExecutionAlgebra

data FromJSONColumn = FromJSONColumn {
    deserializedName :: String,
    deserializedDataType :: String
} deriving (Show, Eq, Generic)

data FromJSONTable = FromJSONTable {
    deserializedtableName :: String,
    deserializedColumns :: [FromJSONColumn],
    deserializedRows :: [[Value]]
} deriving (Show, Eq, Generic)

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

type InsertedValues = [Value]

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement2 sql of
  Left err -> return $ Left err
  Right (ShowTable table) -> do
    content <- loadFile table
    return $ Left content
  Right (Insert table columns values) -> do
    content <- loadFile table
    let dfs = [DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]], DataFrame [Column "value" BoolType, Column "flag" StringType] [[BoolValue True, StringValue "b"],[BoolValue True, StringValue "b"],[BoolValue True, StringValue "b"]]]
    -- case content of
    --         Left err1 -> return $ Left err1
    --         Right deserializedContent -> do
    case columns of
      Just columnsProvided -> insertColumnsProvided ("employees",last dfs) columnsProvided values
      Nothing -> insertToAll ("employees", last dfs) values
  Right (Update table selectUpdate selectWhere) -> do
    content <- loadFile table
    let dfs = [DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]], DataFrame [Column "value" BoolType, Column "flag" StringType] [[BoolValue True, StringValue "b"],[BoolValue True, StringValue "b"],[BoolValue True, StringValue "b"]]]
    let condi = [Condition(ConstantOperand(IntegerValue 1)) IsEqualTo (ConstantOperand(IntegerValue 1))]
    -- case content of
    --         Left err1 -> return $ Left err1
    --         Right deserializedContent -> do
    case selectWhere of
      Just selectWhere ->
          case filterSelectVol2 (last dfs) selectUpdate selectWhere of
            Right df -> return $ Right df
            Left err -> return $ Left err
      Nothing -> case filterSelectVol2 (last dfs) selectUpdate condi of
        Right df -> return $ Right df
        Left err -> return $ Left err
  Right (Delete table conditions) -> do
    let df = DataFrame [Column "flag" StringType, Column "nr" IntegerType] [[StringValue "d", IntegerValue 3],[StringValue "c",IntegerValue 2]]
    case deleteExecution df $ conditions  of
      Right dfs -> return $ Right dfs
      Left err -> return $ Left err

  Right SelectNow -> do
    da <- getTime
    let df = createNowDataFrame (uTCToString da)
    return $ Right df


---------------------------------------some update stuff starts--------------------------

filterConditionVol2 :: [Column] -> [Row] -> Condition -> [Condition] -> [Row]
filterConditionVol2 _ [] _ _ = []
filterConditionVol2 columns (x:xs) condition selectUpdate =
  if conditionResult columns x condition
    then [changeByRequest columns x selectUpdate] ++ filterConditionVol2 columns xs condition selectUpdate
    else [x] ++ filterConditionVol2 columns xs condition selectUpdate

changeByRequest :: [Column] -> Row -> [Condition] -> Row
changeByRequest df row [] = row
changeByRequest columns row (condition:conditions) =
  changeByRequest columns (forOneCondition columns row condition) conditions

forOneCondition :: [Column] -> Row -> Condition -> Row
forOneCondition columns row condition = do
  let conditions = whereConditionColumnName condition
  let values = whereConditionValues condition
  (if hasTwoColumnNames conditions then row else if hasTwoValues values then row else changeByRequestForOne columns row conditions values)

changeByRequestForOne :: [Column] -> [Value] -> [ColumnName] -> [Value] -> [Value]
changeByRequestForOne columns row setColumn setValue = do
  let index = findColumnIndex (head setColumn) columns
  let newRow = replaceNth index (head setValue) row
  newRow

replaceNth :: Int -> Value -> [Value] -> [Value]
replaceNth _ _ [] = []
replaceNth n newVal (x:xs)
  | n == 0 = newVal:xs
  | otherwise = x:replaceNth (n-1) newVal xs

whereConditionValues :: Condition -> [Value]
whereConditionValues (Condition op1 _ op2) =
  case op1 of
    ConstantOperand name1 -> case op2 of
      ConstantOperand name2 -> [name1] ++ [name2]
      _ -> [name1]
    ColumnOperand _ -> case op2 of
      ConstantOperand name -> [name]
      ColumnOperand _ -> []

hasTwoValues :: [Value] -> Bool
hasTwoValues values = length values == 2

hasTwoColumnNames :: [ColumnName] -> Bool
hasTwoColumnNames columnNames = length columnNames == 2


filterSelectVol2 :: DataFrame -> [Condition] -> [Condition] -> Either ErrorMessage DataFrame
filterSelectVol2 df [] _ = Right df
filterSelectVol2 (DataFrame colsOg rowsOg) selectUpdate (x:xs) =
  filterSelectVol2 (DataFrame colsOg $ filterConditionVol2 colsOg rowsOg x selectUpdate) xs selectUpdate
filterSelectVol2 _ _ _ = Left "Error"


---------------------------------------some update stuff ends----------------------------

---------------------------------------some insert stuff starts----------------------------

insertColumnsProvided :: DeserializedContent -> SelectedColumns -> InsertedValues -> Execution (Either ErrorMessage DataFrame)
insertColumnsProvided deserializedContent justColumns values =
  if insertCheckCounts justColumns values then (case insertColumnsProvidedDeserializedContent deserializedContent justColumns values of
    Left errMsg -> return $ Left errMsg
    Right updatedDataFrame -> return (Right updatedDataFrame)) else return $ Left "Behold! I, your sovereign, detect errors in thy counts of values and columns. Attend swiftly, rectify this ledger amiss, for accuracy befits our royal domain."

insertColumnsProvidedDeserializedContent :: DeserializedContent -> SelectedColumns -> InsertedValues -> Either ErrorMessage DataFrame
insertColumnsProvidedDeserializedContent (tableName, DataFrame columns rows) changedColumns newValues =
    Right $ DataFrame columns (rows ++ [insertColumnsProvidedRecurssion columns changedColumns newValues])

insertColumnsProvidedRecurssion :: [Column] -> SelectedColumns -> InsertedValues -> [Value]
insertColumnsProvidedRecurssion [] _ _ = []
insertColumnsProvidedRecurssion (Column colName _ : restColumns) (ColumnsSelected changedColumns) newValues =
    case elemIndex colName changedColumns of
        Just index -> newValues !! index : restValues
        Nothing -> NullValue : restValues
  where
    restValues = insertColumnsProvidedRecurssion restColumns (ColumnsSelected changedColumns) newValues

insertToAll :: DeserializedContent -> [Value] -> Execution (Either ErrorMessage DataFrame)
insertToAll (tableName, loadedDataFrame) values = do
    let checkCount = insertToAllCheckCounts loadedDataFrame values
    either
        (return . Left)
        (\newValues -> do
            let updatedDataFrame = insertToAllDataFrame loadedDataFrame newValues
            --saveTable (tableName, updatedDataFrame)
            return $ Right updatedDataFrame)
        checkCount

insertCheckCounts :: SelectedColumns -> InsertedValues -> Bool
insertCheckCounts (ColumnsSelected colNames) values = length values == length colNames

insertToAllCheckCounts :: DataFrame -> [Value] -> Either ErrorMessage [Value]
insertToAllCheckCounts (DataFrame columns _) values
    | length values == length columns = Right values
    | otherwise = Left "The columns count is not the same as the provided values count"

insertToAllDataFrame :: DataFrame -> [Value] -> DataFrame
insertToAllDataFrame (DataFrame columns rows) newValues =
    DataFrame columns (rows ++ [newValues])


---------------------------------------some insert stuff ends----------------------------

---------------------------------------some delete stuff starts--------------------------

deleteExecution :: DataFrame -> Maybe [Condition] -> Either ErrorMessage DataFrame
deleteExecution contains Nothing = Right $ leaveOnlyColumnNames contains
deleteExecution contains (Just conditions) =
  case isFaultyConditions conditions of
    False -> case doColumnsExist (whereConditionColumnList conditions) contains of
      True -> case areRowsEmpty (filterSelect contains conditions) of
        False -> Right (uncurry createSelectDataFrame (getColumnsRows (columnsToList (contains)) (reversedFilterSelect (contains) conditions)))
        True -> Right $ contains
      False -> Left "The specified column doesn't exist"
    True -> Left "Conditions are faulty"

leaveOnlyColumnNames :: DataFrame -> DataFrame
leaveOnlyColumnNames (DataFrame columns _) = DataFrame columns []

reversedFilterSelect :: DataFrame -> [Condition] -> DataFrame
reversedFilterSelect df [] = df
reversedFilterSelect (DataFrame colsOg rowsOg) (x:xs) = reversedFilterSelect (DataFrame colsOg $ reversedFilterCondition colsOg rowsOg x) xs

reversedFilterCondition :: [Column] -> [Row] -> Condition -> [Row]
reversedFilterCondition _ [] _ = []
reversedFilterCondition columns (x:xs) condition =
  if conditionResult columns x condition
    then  reversedFilterCondition columns xs condition
    else [x] ++ reversedFilterCondition columns xs condition

---------------------------------------some delete stuff ends----------------------------

createNowDataFrame :: [Row] -> DataFrame
createNowDataFrame time = DataFrame [Column "Now" StringType] time

uTCToString :: UTCTime -> [Row]
uTCToString utcTime = [[StringValue (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" (addUTCTime (120*60) utcTime))]]

-------------------------------some JSON shit------------------------------------

toJSONtable :: (TableName, (DataFrame)) -> String
toJSONtable table = "{\"Table\":\"" ++ show (fst (table)) ++ "\",\"Columns\":[" ++ show (toJSONColumns (toColumnList (snd (table)))) ++ "],\"Rows\":[" ++ show (toJSONRows (toRowList (snd (table)))) ++"]}"

toJSONColumn :: Column -> String
toJSONColumn column = "{\"Name\":\"" ++ show (getColumnName (column)) ++ ",\"ColumnType\":\"" ++ show (getType (column)) ++ "\"}"

toJSONRowValue :: Value -> String
toJSONRowValue value = "{\"Value\":\"" ++ show (value) ++ "\"}"

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
-- --C:\Users\Gita\Documents\GitHub\fp-2023\db\flags.json
toFilePath :: TableName -> FilePath
toFilePath tableName = "db/" ++ show (tableName) ++ ".json" --".txt"

writeTableToFile :: (TableName, DataFrame) -> IO ()
writeTableToFile table = writeFile (toFilePath (fst (table))) (toJSONtable (table))

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
    selectUpdate <- optional selectDataParsers
    _ <- optional whitespaceParser
    _ <- queryStatementParser "values"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    values <- insertedValuesParser
    _ <- queryStatementParser ")"
    pure $ Insert tableName selectUpdate values

selectDataParsers :: Parser SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      _ <- queryStatementParser "("
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- queryStatementParser ")"
      return $ ColumnsSelected columnNames

insertedValuesParser :: Parser InsertedValues
insertedValuesParser = do
  values <- seperate valueParser (queryStatementParser "," >> whitespaceParser)
  return $ trace ("Parsed values: " ++ show values) values

valueParser :: Parser Value
valueParser = parseNullValue <|> parseBoolValue <|> parseStringValue <|> parseNumericValue

parseNullValue :: Parser Value
parseNullValue = queryStatementParser "null" >> return NullValue

parseBoolValue :: Parser Value
parseBoolValue =
  (queryStatementParser "true" >> return (BoolValue True))
  <|> (queryStatementParser "false" >> return (BoolValue False))

parseStringValue :: Parser Value
parseStringValue = do
  strValue <- parseStringWithQuotes
  return (StringValue strValue)

parseStringWithQuotes :: Parser String
parseStringWithQuotes = do
  _ <- queryStatementParser "'"
  str <- some (parseSatisfy (/= '\''))
  _ <- queryStatementParser "'"
  return str

parseNumericValue :: Parser Value
parseNumericValue = IntegerValue <$> parseInt

parseInt :: Parser Integer
parseInt = do
  digits <- some (parseSatisfy isDigit)
  return (read digits)

deleteParser :: Parser ParsedStatement2
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser ParsedStatement2
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
    selectUpdated <- setParser
    let selectUpdatedMsg = "Select Updated: " ++ show selectUpdated
    trace selectUpdatedMsg $ pure ()
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParser :: Parser WhereSelect
setParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "set"
  _ <- whitespaceParser
  seperate whereConditionParser (optional whitespaceParser >> char ',' >> optional whitespaceParser)


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
